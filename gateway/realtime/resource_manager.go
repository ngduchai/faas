package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ngduchai/faas/gateway/requests"
	"github.com/ngduchai/faas/gateway/scaling"
	"golang.org/x/build/kubernetes/api"
)

// ResourceManager used by AdmissionControl policies for handling low
// level communication with the underlying container managager.
type ResourceManager struct{}

// CreateImage create a container image for function deployment
func (rm ResourceManager) CreateImage(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	if err := rm.convertRequest(r); err != nil {
		return nil, err
	}

	method := r.Method
	r.Method = http.MethodPost
	res, err := processRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	r.Method = method
	return res, err
}

// RemoveImage remove function decription and image from container
// manager, used to support unregistration operation.
func (rm ResourceManager) RemoveImage(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	method := r.Method
	r.Method = http.MethodDelete
	res, err := processRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	r.Method = method
	return res, err
}

// UpdateImage modifies an already existed image in the underlying
// container manager
func (rm ResourceManager) UpdateImage(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	if err := rm.convertRequest(r); err != nil {
		return nil, err
	}

	method := r.Method
	r.Method = http.MethodPut
	res, err := processRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	r.Method = method
	return res, err
}

// convert request to the form that the unverlying container can
// understand
func (rm ResourceManager) convertRequest(r *http.Request) error {
	// Extract raw request from HTTP body
	req, err := rm.ParseCreateFunctionRequest(r)
	if err != nil {
		return err
	}
	if req.Timeout == 0 {
		req.Timeout = 3 // Similar to current AWS
	}
	timeout := strconv.FormatUint(req.Timeout, 10)
	concurrency, err := rm.ContainerConcurrency(&req)
	if err != nil {
		return err
	}
	if req.Labels == nil {
		req.Labels = &map[string]string{}
	}
	(*req.Labels)["realtime"] = strconv.FormatFloat(req.Realtime, 'f', -1, 64)
	(*req.Labels)["concurrency"] = strconv.FormatInt(int64(concurrency), 10)
	(*req.Labels)["timeout"] = timeout

	req.EnvVars["exec_timeout"] = timeout
	req.EnvVars["read_timeout"] = timeout
	req.EnvVars["write_timeout"] = timeout

	return rm.DumpCreateFunctionRequest(r, &req)
}

func processRequest(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	res, err := forwardRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	if err != nil {
		log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
	} else if res.StatusCode < 200 || res.StatusCode > 299 {
		log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, res.StatusCode)
	}
	return res, err

}

// ParseCreateFunctionRequest parse HTTP request for function update
// or deployment from a readable json format to a concrete
// CreateFunctionRequest object
func (rm ResourceManager) ParseCreateFunctionRequest(req *http.Request) (requests.CreateFunctionRequest, error) {
	request := requests.CreateFunctionRequest{}

	// Extract raw request from HTTP body
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Cannot read parameters: %s\n", err.Error())
		return request, err
	}
	// Parse the request but still make sure the request is still readable
	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
	err = json.Unmarshal(body, &request)
	req.Body = rdr
	if err != nil {
		log.Printf("Cannot read JSON params: %s", err.Error())
		return request, err
	}
	return request, nil
}

// DumpCreateFunctionRequest dumps function description as a
// CreateFunctionRequest object to HTTP body in json format.
func (rm ResourceManager) DumpCreateFunctionRequest(
	req *http.Request,
	desc *requests.CreateFunctionRequest) error {

	body, err := json.Marshal(desc)
	if err != nil {
		return err
	}
	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
	req.Body = rdr
	return nil
}

// Scale a function by either increasing or decreasing its replicas
func (rm ResourceManager) Scale(functionName string, realtimeReplicas uint64) error {
	f := scaling.GetScalerInstance()

	start := time.Now()

	scaleResult := backoff(func(attempt int) error {
		queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
		if err != nil {
			return err
		}

		f.Cache.Set(functionName, queryResponse)

		log.Printf("[Scale %d] function=%s --> %d requested", attempt, functionName, realtimeReplicas)
		setScaleErr := f.Config.ServiceQuery.SetReplicas(functionName, realtimeReplicas)
		if setScaleErr != nil {
			return fmt.Errorf("unable to scale function [%s], err: %s", functionName, setScaleErr)
		}

		return nil

	}, int(f.Config.SetScaleRetries), f.Config.FunctionPollInterval)

	if scaleResult != nil {
		return scaleResult
	}

	for i := 0; i < int(f.Config.MaxPollCount); i++ {
		queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
		if err == nil {
			f.Cache.Set(functionName, queryResponse)
		}
		totalTime := time.Since(start)

		if err != nil {
			return err
		}

		if queryResponse.Replicas >= realtimeReplicas {

			log.Printf("[Scale] function=%s 0 => %d successful - %f seconds",
				functionName, queryResponse.Replicas, totalTime.Seconds())

			return nil
		}

		time.Sleep(f.Config.FunctionPollInterval)
	}
	return nil

}

// GetRealtimeParams returns real-time parameters of a function
func (rm ResourceManager) GetRealtimeParams(
	functionName string) (float64, uint64, uint64, uint64, error) {

	f := scaling.GetScalerInstance()
	scaleInfo, err := f.Config.ServiceQuery.GetReplicas(functionName)

	if err != nil {
		return 0, 0, 0, 60, err
	}

	f.Cache.Set(functionName, scaleInfo)

	return scaleInfo.Realtime, scaleInfo.Concurrency, scaleInfo.Timeout, scaleInfo.Replicas, nil
}

// GetAvailReplicas gets available replicas of a function */
func (rm ResourceManager) GetAvailReplicas(functionName string) (uint64, error) {
	f := scaling.GetScalerInstance()
	queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
	if err != nil {
		return 0, err
	}
	return queryResponse.AvailableReplicas, nil
}

// WaitForAvailReplicas waits until a given function has
// sufficient replicas or timeout determined by retry*interval
func (rm ResourceManager) WaitForAvailReplicas(
	functionName string,
	expectedReplicas uint64,
	retry uint64,
	interval int) bool {
	prevAvail := uint64(0)
	attempt := uint64(0)
	for retry > attempt {
		availReplicas, err := rm.GetAvailReplicas(functionName)
		log.Printf("Attempt #%d, avail: %d, need: %d", attempt, availReplicas, expectedReplicas)
		if err == nil {
			if availReplicas == expectedReplicas {
				return true
			}
			if availReplicas > prevAvail {
				attempt = 0
			}
		}
		attempt++
		prevAvail = availReplicas
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
	return false
}

// ContainerConcurrency returns the number of running functions can be
// multiplexed into a single container.
func (rm ResourceManager) ContainerConcurrency(req *requests.CreateFunctionRequest) (int, error) {
	// In current implementation, we determine function and container
	// size only the min of memory and cpu consumption

	if req.Limits == nil {
		mem := "512 Mi"
		cpu := "1.0"
		if req.Requests != nil {
			if len(req.Requests.CPU) > 0 {
				cpu = req.Requests.CPU
			}
			if len(req.Requests.Memory) > 0 {
				mem = req.Requests.Memory
			}
		}
		req.Limits = &requests.FunctionResources{
			Memory: mem,
			CPU:    cpu,
		}
	}
	// Parse CPU limits
	containerCPU, err := strconv.ParseFloat(req.Limits.CPU, 32)
	funcCPU, err := strconv.ParseFloat(req.Resources.CPU, 32)
	if err != nil {
		return 0, err
	}

	// Parse Memory limits
	containerMemory, err := api.ParseQuantity(req.Limits.Memory)
	funcMemory, err := api.ParseQuantity(req.Resources.Memory)
	if err != nil {
		return 0, err
	}

	// Ensure that function size is not zero
	if funcCPU == 0 || funcMemory.Value() == 0 {
		return 0, errors.New("Function resources must be non-zero")
	}
	sizeCPU := int(containerCPU / funcCPU)
	sizeMem := int(float64(containerMemory.Value()) / float64(funcMemory.Value()))
	size := sizeCPU
	if sizeCPU > sizeMem {
		size = sizeMem
	}
	return size, nil

}

//func forwardRequest(w http.ResponseWriter, r *http.Request, proxyClient *http.Client, baseURL string, requestURL string, timeout time.Duration, writeRequestURI bool) (int, error) {
func forwardRequest(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	upstreamReq := buildUpstreamRequest(r, baseURL, requestURL)
	if upstreamReq.Body != nil {
		defer upstreamReq.Body.Close()
	}

	if writeRequestURI {
		log.Printf("forwardRequest: %s %s\n", upstreamReq.Host, upstreamReq.URL.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, resErr := proxyClient.Do(upstreamReq.WithContext(ctx))

	if res.Body != nil {
		defer res.Body.Close()
	}

	return res, resErr
}

func buildUpstreamRequest(r *http.Request, baseURL string, requestURL string) *http.Request {
	url := baseURL + requestURL

	if len(r.URL.RawQuery) > 0 {
		url = fmt.Sprintf("%s?%s", url, r.URL.RawQuery)
	}

	upstreamReq, _ := http.NewRequest(r.Method, url, nil)

	copyHeaders(upstreamReq.Header, &r.Header)
	deleteHeaders(&upstreamReq.Header, &hopHeaders)

	if len(r.Host) > 0 && upstreamReq.Header.Get("X-Forwarded-Host") == "" {
		upstreamReq.Header["X-Forwarded-Host"] = []string{r.Host}
	}
	if upstreamReq.Header.Get("X-Forwarded-For") == "" {
		upstreamReq.Header["X-Forwarded-For"] = []string{r.RemoteAddr}
	}

	if r.Body != nil {
		upstreamReq.Body = r.Body
	}

	return upstreamReq
}

func copyHeaders(destination http.Header, source *http.Header) {
	for k, v := range *source {
		vClone := make([]string, len(v))
		copy(vClone, v)
		(destination)[k] = vClone
	}
}

func deleteHeaders(target *http.Header, exclude *[]string) {
	for _, h := range *exclude {
		target.Del(h)
	}
}

// Hop-by-hop headers. These are removed when sent to the backend.
// As of RFC 7230, hop-by-hop headers are required to appear in the
// Connection header field. These are the headers defined by the
// obsoleted RFC 2616 (section 13.5.1) and are used for backward
// compatibility.
// Copied from: https://golang.org/src/net/http/httputil/reverseproxy.go
var hopHeaders = []string{
	"Connection",
	"Proxy-Connection", // non-standard but still sent by libcurl and rejected by e.g. google
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",      // canonicalized version of "TE"
	"Trailer", // not Trailers per URL above; https://www.rfc-editor.org/errata_search.php?eid=4522
	"Transfer-Encoding",
	"Upgrade",
}

type routine func(attempt int) error

func backoff(r routine, attempts int, interval time.Duration) error {
	var err error

	for i := 0; i < attempts; i++ {
		res := r(i)
		if res != nil {
			err = res

			log.Printf("Attempt: %d, had error: %s\n", i, res)
		} else {
			err = nil
			break
		}
		time.Sleep(interval)
	}
	return err
}
