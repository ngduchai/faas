package realtime

import (
	"errors"
	"io"
	"log"
	"math"
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
	functionName, realtime, size, duration, error := rm.RequestRealtimeParams(r)
	if error != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		error = errors.New("Function parametera are invalid")
		return statusCode, error
	}
	numReplicas := uint64(math.Ceil(realtime * size * float64(duration)))
	if numReplicas < 1 {
		numReplicas = 1
	}

	// Create function image
	res, error := rm.CreateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if error != nil {
		if res.Body != nil {
			io.CopyBuffer(w, res.Body, nil)
		}
		copyHeaders(w.Header(), &res.Header)
		w.WriteHeader(res.StatusCode)
		return res.StatusCode, error
	}

	statusCode := http.StatusAccepted
	// The function is deployed successfully, now we need to scale to enforce the
	// guaranteed invocation rate
	if realtime > 0 {
		canScale := false
		// Scale!
		log.Printf("Scale function %s to %d\n", functionName, numReplicas)
		error = rm.Scale(functionName, numReplicas)
		if error == nil {
			retries := numReplicas * 2
			if retries < 10 {
				retries = 10
			}
			canScale = rm.WaitForAvailReplicas(functionName, numReplicas, retries, 1000)
		}
		// Check if we can scale successfully
		if !canScale {
			log.Printf("Cannot scale\n")
			// If we fail to scale the function, then rollback the deployment
			_, error := rm.RemoveImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
			if error != nil {
				log.Printf("Unable to rollback function %s deployment", functionName)
			}
			statusCode = http.StatusInternalServerError
			error = errors.New("Insuffcient resources. Cancel deployment!\n")
		}
	}
	w.WriteHeader(statusCode)
	if error != nil {
		w.Write([]byte(error.Error()))
	}
	return statusCode, error
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
	functionName, realtime, size, duration, error := rm.RequestRealtimeParams(r)
	if error != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		error = errors.New("Function parametera are invalid")
		return statusCode, error
	}
	numReplicas := uint64(math.Ceil(realtime * size * float64(duration)))
	if numReplicas < 1 {
		numReplicas = 1
	}

	// Capture the current real-time parameter if backoff is needed
	prevRealtime, prevSize, prevDuration, prevReplicas, error := rm.DeploymentRealtimeParams(functionName)
	if error != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		return statusCode, error
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
	if prevRealtime > 0 || realtime > 0 {
		// Scale!
		canScale := false
		error = rm.Scale(functionName, numReplicas)
		if error == nil {
			if prevRealtime < realtime {
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
			error = rm.SetRealtimeParams(r, prevRealtime, prevSize, prevDuration)
			if error != nil {
				log.Printf("Unable set back real-time params: %s", error.Error())
			}
			// If we fail to scale the function, then rollback the deployment
			_, error := rm.UpdateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
			if error != nil {
				log.Printf("Unable to rollback function %s update", functionName)
			}
			error = rm.Scale(functionName, prevReplicas)
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
