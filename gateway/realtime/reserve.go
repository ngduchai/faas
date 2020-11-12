package realtime

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type ReserveAdmissionControl struct {
}

func (ac ReserveAdmissionControl) Register(
	w http.ResponseWriter,
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (int, error) {

	// Deploy throught the resource manager interface
	rm := ResourceManager{}

	// Get realtime params
	// functionName, realtime, cpu, memory, duration, error := rm.RequestRealtimeParams(r)
	request, err := rm.ParseRequest(r)
	if err != nil {
		log.Printf("Reading parameters error: %s", err)
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		err = errors.New("Function parameters are invalid")
		return statusCode, err
	}
	// numReplicas := uint64(math.Ceil(realtime * size * float64(duration)))
	// if numReplicas < 1 {
	// 	numReplicas = 1
	// }
	// numReplicas := uint64(1)
	cpus, memory, err := rm.GetResourceQuantity(*request.Resources)
	if err != nil {
		log.Printf("Reading parameters error: %s", err)
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		err = errors.New("Function parameters are invalid")
		return statusCode, err
	}
	//numReplicas := 1
	totalCPU := int64(request.Realtime * float64(cpus) * float64(request.Timeout) / 1000)
	totalMemory := int64(request.Realtime * float64(memory) * float64(request.Timeout) / 1000)
	log.Printf("CPU: %d Memory %d", totalCPU, totalMemory)
	if request.Realtime > 0 {
		rm.SetSandboxResources(&request, totalCPU, totalMemory)
	}
	rm.PackageRequest(request, r)
	res, err := rm.CreateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if err != nil {
		if res.Body != nil {
			io.CopyBuffer(w, res.Body, nil)
		}
		copyHeaders(w.Header(), &res.Header)
		w.WriteHeader(res.StatusCode)
		return res.StatusCode, err
	}

	statusCode := http.StatusAccepted
	// The function is deployed successfully, now we need to scale to enforce the
	// guaranteed invocation rate
	// if realtime > 0 {
	// 	canScale := false
	// 	// Scale!
	// 	log.Printf("Scale function %s to %d\n", functionName, numReplicas)
	// 	error = rm.Scale(functionName, numReplicas)
	// 	if error == nil {
	// 		retries := numReplicas * 2
	// 		if retries < 10 {
	// 			retries = 10
	// 		}
	// 		canScale = rm.WaitForAvailReplicas(functionName, numReplicas, retries, 1000)
	// 	}
	// 	// Check if we can scale successfully
	// 	if !canScale {
	// 		log.Printf("Cannot scale\n")
	// 		// If we fail to scale the function, then rollback the deployment
	// 		_, error := rm.RemoveImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	// 		if error != nil {
	// 			log.Printf("Unable to rollback function %s deployment", functionName)
	// 		}
	// 		statusCode = http.StatusInternalServerError
	// 		error = errors.New("Insuffcient resources. Cancel deployment")
	// 	}
	// }
	w.WriteHeader(statusCode)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	return statusCode, err
}

func (ac ReserveAdmissionControl) Update(
	w http.ResponseWriter,
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (int, error) {

	// Update through the resource manager interface
	rm := ResourceManager{}

	// First, extract real-time parameters from the request
	request, err := rm.ParseRequest(r)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		err = errors.New("Function parameters are invalid")
		return statusCode, err
	}
	// numReplicas := uint64(math.Ceil(realtime * size * float64(duration)))
	// if numReplicas < 1 {
	// 	numReplicas = 1
	// }
	functionName := request.Service
	numReplicas := uint64(1)
	cpus, memory, err := rm.GetResourceQuantity(*request.Resources)
	if err != nil {
		log.Printf("Reading parameters error: %s", err)
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		err = errors.New("Function parameters are invalid")
		return statusCode, err
	}
	//numReplicas := 1
	totalCPU := int64(request.Realtime * float64(cpus) * float64(request.Timeout) / 1000)
	totalMemory := int64(request.Realtime * float64(memory) * float64(request.Timeout) / 1000)
	log.Printf("CPU: %d Memory %d", totalCPU, totalMemory)
	if request.Realtime > 0 {
		rm.SetSandboxResources(&request, totalCPU, totalMemory)
	}
	rm.PackageRequest(request, r)

	// Capture the current real-time parameter if backoff is needed
	prevParams, err := rm.GetDeploymentParams(functionName)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		return statusCode, err
	}

	// Update the image
	res, error := rm.UpdateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if error != nil {
		if res.Body != nil {
			io.CopyBuffer(w, res.Body, nil)
		}
		copyHeaders(w.Header(), &res.Header)
		w.WriteHeader(res.StatusCode)
		return res.StatusCode, error
	}
	statusCode := http.StatusAccepted
	if prevParams.Realtime > 0 || request.Realtime > 0 {
		// Scale!
		canScale := false
		error = rm.Scale(functionName, numReplicas)
		if error == nil {
			if prevParams.Realtime < request.Realtime {
				// Only wait for scale up
				retries := numReplicas * 2
				if retries < 10 {
					retries = 10
				}
				canScale = rm.WaitForAvailReplicas(functionName, numReplicas, retries, 1000)
			} else {
				// For scale down, just releasing unused replicas, no need to wait
				canScale = true
			}
		}

		// Check if we can scale successfully
		if !canScale {
			// Set back the real time parameter
			request.Realtime = prevParams.Realtime
			request.Resources.CPU = fmt.Sprintf("%vm", prevParams.CPU)
			request.Resources.Memory = fmt.Sprint(prevParams.Memory)
			request.Timeout = prevParams.Duration
			totalCPU = int64(request.Realtime * float64(prevParams.CPU) * float64(request.Timeout) / 1000)
			totalMemory = int64(request.Realtime * float64(prevParams.Memory) * float64(request.Timeout) / 1000)
			rm.SetSandboxResources(&request, totalCPU, totalMemory)
			error = rm.PackageRequest(request, r)
			if error != nil {
				log.Printf("Unable set back real-time params: %s", error.Error())
			}
			// If we fail to scale the function, then rollback the deployment
			_, error := rm.UpdateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
			if error != nil {
				log.Printf("Unable to rollback function %s update", functionName)
			}
			error = rm.Scale(functionName, prevParams.Replicas)
			if error != nil {
				log.Printf("Unable to rollback function %s scale", functionName)
			}
			canScale = rm.WaitForAvailReplicas(functionName, numReplicas, numReplicas*2, 1)
			if !canScale {
				log.Printf("Unable to scale back after update %s", functionName)
			}
			statusCode = http.StatusInternalServerError
			error = errors.New("Insuffcient resources. Cancel update the function!\n")
		}
	}
	w.WriteHeader(statusCode)
	if error != nil {
		w.Write([]byte(error.Error()))
	}
	return statusCode, error

}

func (ac ReserveAdmissionControl) Unregister(
	w http.ResponseWriter,
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (int, error) {

	// Remove throught the resource manager interface
	rm := ResourceManager{}

	// Remove function image
	res, error := rm.RemoveImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if error != nil {
		if res.Body != nil {
			io.CopyBuffer(w, res.Body, nil)
		}
		copyHeaders(w.Header(), &res.Header)
		w.WriteHeader(res.StatusCode)
	}
	return http.StatusAccepted, nil
}
