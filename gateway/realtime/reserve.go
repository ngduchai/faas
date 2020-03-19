package realtime

import (
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"time"
)

// ReserveAdmissionControl implements the AdmissionControl by reserving
// resources at function deployment
type ReserveAdmissionControl struct {
}

// Register registers a function, after the call, the function should
// be ready for invocation requests
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
	req, err := rm.ParseCreateFunctionRequest(r)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		w.Write([]byte(err.Error()))
		err = errors.New("Function parameters are invalid")
		return statusCode, err
	}
	concurrency, err := rm.ContainerConcurrency(&req)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		w.Write([]byte(err.Error()))
		return statusCode, err
	}
	numReplicas := uint64(math.Ceil(req.Realtime / float64(concurrency) * float64(req.Timeout)))
	if numReplicas < 1 {
		numReplicas = 1
	}

	// Create function image
	res, err := rm.CreateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if err != nil {
		if res.Body != nil {
			io.CopyBuffer(w, res.Body, nil)
		}
		copyHeaders(w.Header(), &res.Header)
		w.WriteHeader(res.StatusCode)
		w.Write([]byte(err.Error()))
		return res.StatusCode, err
	}

	statusCode := http.StatusAccepted
	functionName := req.Service
	// The function is deployed successfully, now we need to scale to enforce the
	// guaranteed invocation rate
	if req.Realtime > 0 {
		canScale := false
		// Scale!
		log.Printf("Scale function %s to %d\n", functionName, numReplicas)
		err = rm.Scale(functionName, numReplicas)
		if err == nil {
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
			error = errors.New("insuffcient resources, cancel deployment")
		}
	}
	w.WriteHeader(statusCode)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	return statusCode, err
}

// Update modifies a registered function
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
	req, err := rm.ParseCreateFunctionRequest(r)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		err = errors.New("Function parametera are invalid")
		return statusCode, err
	}
	concurrency, err := rm.ContainerConcurrency(&req)
	if err != nil {
		statusCode := http.StatusNotFound
		w.WriteHeader(statusCode)
		return statusCode, err
	}
	numReplicas := uint64(math.Ceil(req.Realtime / float64(concurrency) * float64(req.Timeout)))
	if numReplicas < 1 {
		numReplicas = 1
	}

	functionName := req.Service
	// Capture the current real-time parameter if backoff is needed
	prevRealtime, _, _, prevReplicas, error := rm.GetRealtimeParams(functionName)
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
	if prevRealtime > 0 || req.Realtime > 0 {
		// Scale!
		canScale := false
		error = rm.Scale(functionName, numReplicas)
		if error == nil {
			if prevRealtime < req.Realtime {
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
			req.Realtime = prevRealtime
			// Then rollback realtime guarantee
			if err = rm.DumpCreateFunctionRequest(r, &req); err != nil {
				log.Printf("Unable set back real-time params: %s", err.Error())
			} else {
				if _, err := rm.UpdateImage(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI); err != nil {
					log.Printf("Unable to rollback function %s update", functionName)
				} else {
					if err = rm.Scale(functionName, prevReplicas); err != nil {
						log.Printf("Unable to rollback function %s scale", functionName)
					} else {
						if !rm.WaitForAvailReplicas(functionName, numReplicas, numReplicas*2, 1) {
							log.Printf("Unable to scale back after update %s", functionName)
						}
					}

				}
			}
			statusCode = http.StatusInternalServerError
			err = errors.New("insuffcient resources, cancel update the function")
		}
	}
	w.WriteHeader(statusCode)
	if error != nil {
		w.Write([]byte(error.Error()))
	}
	return statusCode, error

}

// Unregister removes a deployed function
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
