package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ngduchai/faas/gateway/requests"
	"github.com/ngduchai/faas/gateway/scaling"
	"k8s.io/apimachinery/pkg/api/resource"
)

type ResourceManager struct{}

func (rm ResourceManager) CreateImage(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	method := r.Method
	r.Method = http.MethodPost
	res, err := processRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	r.Method = method
	return res, err
}

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

func (rm ResourceManager) UpdateImage(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	method := r.Method
	r.Method = http.MethodPut
	res, err := processRequest(r, proxyClient, baseURL, requestURL, timeout, writeRequestURI)
	r.Method = method
	return res, err
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

// ParseRequest returns a CreateFunctionRequest that hold all needed information about function deployment/update
func (rm ResourceManager) ParseRequest(req *http.Request) (requests.CreateFunctionRequest, error) {
	request := requests.CreateFunctionRequest{}
	body, err := ioutil.ReadAll(req.Body)
	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
	if err == nil {
		log.Printf("Message: %s", body)
		err = json.Unmarshal(body, &request)
	}
	req.Body = rdr
	return request, err
}

// PackageRequest reformats the request into the form understandable by the underlying system and write to the http request
func (rm ResourceManager) PackageRequest(cfr requests.CreateFunctionRequest, req *http.Request) error {
	if cfr.Labels == nil {
		cfr.Labels = &map[string]string{}
	}
	// Update label for later retrivals
	(*cfr.Labels)["realtime"] = fmt.Sprint(cfr.Realtime)
	(*cfr.Labels)["cpu"] = fmt.Sprint(cfr.Resources.CPU)
	(*cfr.Labels)["memory"] = fmt.Sprint(cfr.Resources.Memory)
	(*cfr.Labels)["duration"] = fmt.Sprint(cfr.Timeout)

	// Update timeout labels
	if cfr.Timeout > 0 {
		if cfr.EnvVars == nil {
			cfr.EnvVars = map[string]string{}
		}
		t := float32(cfr.Timeout) / 1000.0
		cfr.EnvVars["exec_timeout"] = fmt.Sprint(t)
		cfr.EnvVars["read_timeout"] = fmt.Sprint(t)
		cfr.EnvVars["write_timeout"] = fmt.Sprint(t)
	}
	body, err := json.Marshal(cfr)
	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
	req.Body = rdr
	return err
}

// Return realtime, functionsize (= function/replicas), and duration
// func (rm ResourceManager) RequestRealtimeParams(req *http.Request) (string, float64, int64, int64, uint64, error) {
// 	cpus := int64(1000)
// 	memory := int64(256 * 1024 * 1024)
// 	duration := uint64(10000)
// 	body, err := ioutil.ReadAll(req.Body)
// 	if err != nil {
// 		log.Printf("Cannot read parameters: %s\n", err.Error())
// 		return "", 0.0, cpus, memory, duration, err
// 	}
// 	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	request := requests.CreateFunctionRequest{}
// 	log.Printf("Message: %s", body)
// 	err = json.Unmarshal(body, &request)
// 	req.Body = rdr
// 	if err != nil {
// 		log.Printf("Cannot read JSON params: %s", err.Error())
// 		return "", 0.0, cpus, memory, duration, err
// 	}

// 	// if request.Labels == nil {
// 	// 	// Return default value
// 	// 	return request.Service, 0.0, 1.0, 256, 10000, 60, nil
// 	// }
// 	functionName := request.Service
// 	realtime := request.Realtime
// 	if request.Timeout > 0 {
// 		duration = request.Timeout
// 	}
// 	if request.Resources != nil {
// 		cpus, err = rm.GetCPUQuantity(request.Resources.CPU)
// 		if err != nil {
// 			return functionName, realtime, cpus, memory, duration, err
// 		}
// 		memory, err = rm.GetMemoryQuantity(request.Resources.Memory)
// 	}
// 	// // Make sure the resources are all labeled for later uses
// 	// if request.Labels == nil {
// 	// 	request.Labels = &map[string]string{}
// 	// }
// 	// // Update label for later retrivals
// 	// (*request.Labels)["realtime"] = fmt.Sprint(realtime)
// 	// (*request.Labels)["cpu"] = fmt.Sprint(cpus)
// 	// (*request.Labels)["memory"] = fmt.Sprint(memory)
// 	// (*request.Labels)["duration"] = fmt.Sprint(duration)

// 	// realtime := extractLabelRealValue((*request.Labels)["realtime"], float64(0))
// 	// size := extractLabelValue((*request.Labels)["functionsize"], uint64(256))
// 	// duration := extractLabelValue((*request.Labels)["duration"], uint64(60))
// 	log.Printf("realtime: %f duration: %d cpu: %d memory %d", realtime, request.Timeout, cpus, memory)
// 	return functionName, realtime, cpus, memory, duration, err
// }

// GetCPUQuantity returns CPU quantity as number
func (rm ResourceManager) GetCPUQuantity(str string) (int64, error) {
	quantity, err := resource.ParseQuantity(str)
	if err != nil {
		return 0, err
	}
	return quantity.MilliValue(), nil
}

// GetMemoryQuantity returns memory quantity as number
func (rm ResourceManager) GetMemoryQuantity(str string) (int64, error) {
	quantity, err := resource.ParseQuantity(str)
	if err != nil {
		return 0, err
	}
	return quantity.Value(), nil
}

func (rm ResourceManager) GetResourceQuantity(req requests.FunctionResources) (int64, int64, error) {
	cpus, err := rm.GetCPUQuantity(req.CPU)
	if err != nil {
		return 0, 0, err
	}
	mem, err := rm.GetMemoryQuantity(req.Memory)
	return cpus, mem, err
}

// // Return realtime, functionsize (= cpus), and duration
// func (rm ResourceManager) SetRealtimeParams(
// 	req *http.Request,
// 	realtime float64,
// 	cpus int64,
// 	memory int64,
// 	duration uint64) error {

// 	body, _ := ioutil.ReadAll(req.Body)
// 	//rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	request := requests.CreateFunctionRequest{}
// 	err := json.Unmarshal(body, &request)
// 	//req.Body = rdr

// 	if err != nil {
// 		return err
// 	}
// 	if request.Labels == nil {
// 		request.Labels = &map[string]string{}
// 	}
// 	request.Realtime = realtime
// 	request.Timeout = duration
// 	if request.Resources == nil {
// 		request.Resources = &requests.FunctionResources{}
// 	}
// 	request.Resources.CPU = fmt.Sprintf("%vm", cpus)
// 	request.Resources.Memory = fmt.Sprint(memory)

// 	// Update label for later retrivals
// 	(*request.Labels)["realtime"] = fmt.Sprint(realtime)
// 	(*request.Labels)["cpu"] = fmt.Sprint(cpus)
// 	(*request.Labels)["memory"] = fmt.Sprint(memory)
// 	(*request.Labels)["duration"] = fmt.Sprint(duration)

// 	body, err = json.Marshal(request)
// 	if err != nil {
// 		return err
// 	}
// 	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	req.Body = rdr
// 	return nil
// }

// // Reserve resource for realtime deployment
// func (rm ResourceManager) ReserveResource(req *http.Request, cpu int64, memory int64) error {
// 	body, _ := ioutil.ReadAll(req.Body)
// 	//rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	request := requests.CreateFunctionRequest{}
// 	err := json.Unmarshal(body, &request)
// 	//req.Body = rdr

// 	if err != nil {
// 		return err
// 	}

// 	if request.Requests == nil {
// 		request.Requests = &requests.FunctionResources{}
// 	}
// 	request.Requests.CPU = fmt.Sprintf("%vm", cpu)
// 	request.Requests.Memory = fmt.Sprint(memory)
// 	log.Printf("Update resource constraint: %s %s", request.Requests.CPU, request.Requests.Memory)

// 	body, err = json.Marshal(request)
// 	if err != nil {
// 		return err
// 	}
// 	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	req.Body = rdr
// 	return nil
// }

// Reserve resource for realtime deployment
func (rm ResourceManager) SetSandboxResources(request *requests.CreateFunctionRequest, cpu int64, memory int64) {
	if request.Requests == nil {
		request.Requests = &requests.FunctionResources{}
		request.Requests.CPU = fmt.Sprintf("%vm", cpu)
		request.Requests.Memory = fmt.Sprint(memory)
	}

	if request.Limits == nil {
		request.Limits = &requests.FunctionResources{}
		request.Limits.CPU = fmt.Sprintf("%vm", cpu)
		request.Limits.Memory = fmt.Sprint(memory)
	}

	log.Printf("Update resource constraint: %d %d", cpu, memory)
}

// // Reserve resource for realtime deployment
// func (rm ResourceManager) ReserveResource(req *http.Request, cpu int64, memory int64) error {
// 	body, _ := ioutil.ReadAll(req.Body)
// 	//rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	request := requests.CreateFunctionRequest{}
// 	err := json.Unmarshal(body, &request)
// 	//req.Body = rdr

// 	if err != nil {
// 		return err
// 	}

// 	if request.Requests == nil {
// 		request.Requests = &requests.FunctionResources{}
// 	}
// 	request.Requests.CPU = fmt.Sprintf("%vm", cpu)
// 	request.Requests.Memory = fmt.Sprint(memory)
// 	log.Printf("Update resource constraint: %s %s", request.Requests.CPU, request.Requests.Memory)

// 	body, err = json.Marshal(request)
// 	if err != nil {
// 		return err
// 	}
// 	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	req.Body = rdr
// 	return nil
// }

// // Apply runtime restriction for resource limitation
// func (rm ResourceManager) RestrictRuntime(req *http.Request, timeout uint64) error {
// 	body, _ := ioutil.ReadAll(req.Body)
// 	//rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	request := requests.CreateFunctionRequest{}
// 	err := json.Unmarshal(body, &request)
// 	//req.Body = rdr

// 	if err != nil {
// 		return err
// 	}

// 	if request.EnvVars == nil {
// 		request.EnvVars = map[string]string{}
// 	}
// 	t := float32(timeout) / 1000.0
// 	request.EnvVars["exec_timeout"] = fmt.Sprint(t)
// 	request.EnvVars["read_timeout"] = fmt.Sprint(t)
// 	request.EnvVars["write_timeout"] = fmt.Sprint(t)

// 	log.Printf("Restrict runtime to %f", t)

// 	body, err = json.Marshal(request)
// 	if err != nil {
// 		return err
// 	}
// 	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
// 	req.Body = rdr
// 	return nil
// }

/* Scale a function by either increasing or decreasing its replicas */
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

// GetDeploymentParams returns deployment params of a registered function
func (rm ResourceManager) GetDeploymentParams(functionName string) (scaling.ServiceQueryResponse, error) {

	f := scaling.GetScalerInstance()
	scaleInfo, err := f.Config.ServiceQuery.GetReplicas(functionName)

	if err == nil {
		f.Cache.Set(functionName, scaleInfo)
	}

	return scaleInfo, err
}

/* Check if available replicas meet expect ones */
func (rm ResourceManager) GetAvailReplicas(functionName string) (uint64, error) {
	f := scaling.GetScalerInstance()
	queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
	if err != nil {
		return 0, err
	}
	return queryResponse.AvailableReplicas, nil
}

/* Wait until a given function has sufficient replicas or timeout determined by retry*interval */
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

//func forwardRequest(w http.ResponseWriter, r *http.Request, proxyClient *http.Client, baseURL string, requestURL string, timeout time.Duration, writeRequestURI bool) (int, error) {
func forwardRequest(
	r *http.Request,
	proxyClient *http.Client,
	baseURL string,
	requestURL string,
	timeout time.Duration,
	writeRequestURI bool) (*http.Response, error) {

	body, _ := ioutil.ReadAll(r.Body)
	rdr := ioutil.NopCloser(bytes.NewBuffer(body))
	log.Printf("Message: %s", body)
	r.Body = rdr

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

// extractLabelValue will parse the provided raw label value and if it fails
// it will return the provided fallback value and log an message
func extractLabelValue(rawLabelValue string, fallback uint64) uint64 {
	if len(rawLabelValue) <= 0 {
		return fallback
	}

	value, err := strconv.Atoi(rawLabelValue)

	if err != nil {
		log.Printf("Provided label value %s should be of type uint", rawLabelValue)
		return fallback
	}

	return uint64(value)
}

// extractLabelValue will parse the provided raw label value and if it fails
// it will return the provided fallback value and log an message
func extractLabelRealValue(rawLabelValue string, fallback float64) float64 {
	if len(rawLabelValue) <= 0 {
		return fallback
	}

	value, err := strconv.ParseFloat(rawLabelValue, 64)

	if err != nil {
		log.Printf("Provided label value %s should be of type float", rawLabelValue)
		return fallback
	}

	return float64(value)
}
