package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/openfaas/faas/gateway/handlers"
	"github.com/openfaas/faas/gateway/requests"
	"github.com/openfaas/faas/gateway/scaling"
	"github.com/openfaas/faas/gateway/types"
)

//type FunctionRealtimeScaler scaling.FunctionScaler

// Forward the deploy request to the backend. However, we also reserve instances for
// guaranteeing realtime invocation
func MakeRealtimeDeployHandler(proxy *types.HTTPClientReverseProxy, notifiers []handlers.HTTPNotifier, baseURLResolver handlers.BaseURLResolver, urlPathTransformer handlers.URLPathTransformer) http.HandlerFunc {

	writeRequestURI := false
	if _, exists := os.LookupEnv("write_request_uri"); exists {
		writeRequestURI = exists
	}

	//scaler := scaling.GetScalerInstance()

	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := baseURLResolver.Resolve(r)
		originalURL := r.URL.String()

		requestURL := urlPathTransformer.Transform(r)

		start := time.Now()
		body, _ := ioutil.ReadAll(r.Body)
		rdr := ioutil.NopCloser(bytes.NewBuffer(body))
		request := requests.CreateFunctionRequest{}
		err := json.Unmarshal(body, &request)
		r.Body = rdr

		statusCode := http.StatusOK
		if err == nil {
			res, err := forwardRequest(r, proxy.Client, baseURL, requestURL, proxy.Timeout, writeRequestURI)
			statusCode = res.StatusCode
			if err != nil {
				log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
			} else if statusCode < 200 || statusCode > 299 {
				log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, statusCode)
			} else {
				copyHeaders(w.Header(), &res.Header)
				if res.Body != nil {
					// Copy the body over
					io.CopyBuffer(w, res.Body, nil)
				}

				// It seems that function is successfully deployed, but we need to
				// make sure that all necessary information are registered by pulling
				// scaling information
				functionName := request.Service
				// Scale!
				res := RealtimeScale(functionName)

				if !res.Found || res.Error != nil {
					errStr := fmt.Sprintf("error finding function %s: %s", functionName, res.Error.Error())
					log.Printf("Scaling: %s", errStr)

					statusCode = http.StatusInternalServerError
				} else {
					log.Printf("[Deploy] function=%s 0=>%d timed-out after %f seconds", functionName, res.Replicas, res.Duration.Seconds())
				}
			}
		} else {
			errStr := fmt.Sprintf("error deploying: %s", err.Error())
			log.Printf("Deploying: %s", errStr)
			statusCode = http.StatusInternalServerError
		}

		w.WriteHeader(statusCode)

		seconds := time.Since(start)

		// defer func() {
		for _, notifier := range notifiers {
			notifier.Notify(r.Method, requestURL, originalURL, statusCode, seconds)
		}
		// }()

	}
}

// Forward the update request to the backend. However, we also reserve instances for
// guaranteeing realtime invocation
func MakeRealtimeUpdateHandler(proxy *types.HTTPClientReverseProxy, notifiers []handlers.HTTPNotifier, baseURLResolver handlers.BaseURLResolver, urlPathTransformer handlers.URLPathTransformer) http.HandlerFunc {

	writeRequestURI := false
	if _, exists := os.LookupEnv("write_request_uri"); exists {
		writeRequestURI = exists
	}

	//scaler := scaling.GetScalerInstance()

	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := baseURLResolver.Resolve(r)
		originalURL := r.URL.String()

		requestURL := urlPathTransformer.Transform(r)

		start := time.Now()
		body, _ := ioutil.ReadAll(r.Body)
		rdr := ioutil.NopCloser(bytes.NewBuffer(body))
		request := requests.CreateFunctionRequest{}
		err := json.Unmarshal(body, &request)
		r.Body = rdr

		statusCode := http.StatusOK
		if err == nil {
			res, err := forwardRequest(r, proxy.Client, baseURL, requestURL, proxy.Timeout, writeRequestURI)
			statusCode = res.StatusCode
			if err != nil {
				log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
			} else if statusCode < 200 || statusCode > 299 {
				log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, statusCode)
			} else {
				copyHeaders(w.Header(), &res.Header)
				if res.Body != nil {
					// Copy the body over
					io.CopyBuffer(w, res.Body, nil)
				}

				// It seems that function is successfully deployed, but we need to
				// make sure that all necessary information are registered by pulling
				// scaling information
				functionName := request.Service
				// Scale!
				res := RealtimeScale(functionName)

				if !res.Found || res.Error != nil {
					errStr := fmt.Sprintf("error finding function %s: %s", functionName, res.Error.Error())
					log.Printf("Scaling: %s", errStr)

					statusCode = http.StatusInternalServerError
				} else {
					log.Printf("[Update] function=%s 0=>%d timed-out after %f seconds", functionName, res.Replicas, res.Duration.Seconds())
				}
			}
		} else {
			errStr := fmt.Sprintf("error updating: %s", err.Error())
			log.Printf("Updating: %s", errStr)
			statusCode = http.StatusInternalServerError
		}

		w.WriteHeader(statusCode)

		seconds := time.Since(start)

		// defer func() {
		for _, notifier := range notifiers {
			notifier.Notify(r.Method, requestURL, originalURL, statusCode, seconds)
		}
		// }()

	}
}

func MakeRealtimeInvokeHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		originalURL := r.URL.String()
		tokens := strings.Split(originalURL, "/")
		functionName := tokens[len(tokens)-1]
		log.Printf("Invoke function %s", functionName)

		scaler := scaling.GetScalerInstance()

		callid := r.Header.Get("X-Call-Id")
		_, added := scaler.BypassMap.Load(callid)
		if added {
			scaler.BypassMap.Delete(callid)
		}
		numTries := 0
		retryLimit := 1
		for !added && numTries < retryLimit {

			invokeTime := time.Now()
			scaleInfo, hit := scaler.Cache.Get(functionName)
			if !hit {
				scaleInfo, err := scaler.Config.ServiceQuery.GetReplicas(functionName)

				if err == nil {
					scaler.Cache.Set(functionName, scaleInfo)
				} else {
					next(w, r)
					return
				}
			}
			limit := scaleInfo.Realtime
			if limit == 0.0 {
				// Best effort invocation when no guarantee is enforced
				added = true
				break
			}
			//total := limit
			//total := uint64(0)
			//total, gap, added := scaler.Cache.UpdateInvocation(functionName, invokeTime)
			//total, _, added = scaler.Cache.UpdateInvocation(functionName, invokeTime)
			_, _, added = scaler.Cache.UpdateInvocation(functionName, invokeTime)

			if !added {
				//wait := 1.0 - gap.Seconds() + 0.001
				//wait := 1.0
				//wait := 1 / scaleInfo.Realtime // --> try to spread out the requests
				wait := 1 / (100 * scaleInfo.Realtime) // --> try to spread out the requests
				numTries++
				//log.Printf("[Invoke %d] Number of requests = %d >= limit = %f for function %s. Pause invocation by %f second(s)", numTries, total, limit, functionName, wait)
				time.Sleep(time.Duration(wait*1000) * time.Millisecond)
			}
		}

		if !added {
			w.WriteHeader(http.StatusRequestTimeout)
		} else {
			next(w, r)
		}
	}
}

//func forwardRequest(w http.ResponseWriter, r *http.Request, proxyClient *http.Client, baseURL string, requestURL string, timeout time.Duration, writeRequestURI bool) (int, error) {
func forwardRequest(r *http.Request, proxyClient *http.Client, baseURL string, requestURL string, timeout time.Duration, writeRequestURI bool) (*http.Response, error) {

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

	/*
		if resErr != nil {
				badStatus := http.StatusBadGateway
				w.WriteHeader(badStatus)
				return badStatus, resErr
			return res, resErr
		}

		if res.Body != nil {
			defer res.Body.Close()
		}
			copyHeaders(w.Header(), &res.Header)

			// Write status code
			w.WriteHeader(res.StatusCode)

			if res.Body != nil {
				// Copy the body over
				io.CopyBuffer(w, res.Body, nil)
			}

			return res.StatusCode, nil
	*/
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

// Scale scales a function from zero replicas to 1 or the value set in
// the minimum replicas metadata
func RealtimeScale(functionName string) scaling.FunctionScaleResult {
	start := time.Now()

	/*
		scaleInfo := scaling.ServiceQueryResponse{}

		if cachedResponse, hit := f.Cache.Get(functionName); !hit {

			queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)

			if err != nil {
				return scaling.FunctionScaleResult{
					Error:     err,
					Available: false,
					Found:     false,
					Duration:  time.Since(start),
					Replicas:  cachedResponse.Replicas,
				}
			}

			f.Cache.Set(functionName, queryResponse)
			scaleInfo = queryResponse

		} else {
			scaleInfo = cachedResponse
		}
	*/
	f := scaling.GetScalerInstance()

	scaleInfo, err := f.Config.ServiceQuery.GetReplicas(functionName)

	if err != nil {
		return scaling.FunctionScaleResult{
			Error:     err,
			Available: false,
			Found:     false,
			Duration:  time.Since(start),
			Replicas:  scaleInfo.Replicas,
		}
	}

	f.Cache.Set(functionName, scaleInfo)

	realtimeReplicas := uint64(math.Ceil(scaleInfo.Realtime * scaleInfo.FunctionSize * float64(scaleInfo.Duration)))

	log.Printf("Realtime: %f, Duration %d", scaleInfo.Realtime, scaleInfo.Duration)
	log.Printf("Scale: Required: %d, Current %d", realtimeReplicas, scaleInfo.Replicas)

	//if scaleInfo.Replicas < realtimeReplicas {
	if scaleInfo.Replicas != realtimeReplicas && realtimeReplicas > 0 {
		scaleResult := backoff(func(attempt int) error {
			queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
			if err != nil {
				return err
			}

			f.Cache.Set(functionName, queryResponse)

			/*
				if queryResponse.Replicas >= realtimeReplicas {
					return nil
				}
			*/

			log.Printf("[Scale %d] function=%s --> %d requested", attempt, functionName, realtimeReplicas)
			setScaleErr := f.Config.ServiceQuery.SetReplicas(functionName, realtimeReplicas)
			if setScaleErr != nil {
				return fmt.Errorf("unable to scale function [%s], err: %s", functionName, setScaleErr)
			}

			return nil

		}, int(f.Config.SetScaleRetries), f.Config.FunctionPollInterval)

		if scaleResult != nil {
			return scaling.FunctionScaleResult{
				Error:     scaleResult,
				Available: false,
				Found:     true,
				Duration:  time.Since(start),
				Replicas:  0,
			}
		}

		for i := 0; i < int(f.Config.MaxPollCount); i++ {
			queryResponse, err := f.Config.ServiceQuery.GetReplicas(functionName)
			if err == nil {
				f.Cache.Set(functionName, queryResponse)
			}
			totalTime := time.Since(start)

			if err != nil {
				return scaling.FunctionScaleResult{
					Error:     err,
					Available: false,
					Found:     true,
					Duration:  totalTime,
					Replicas:  queryResponse.Replicas,
				}
			}

			if queryResponse.Replicas >= realtimeReplicas {

				log.Printf("[Scale] function=%s 0 => %d successful - %f seconds",
					functionName, queryResponse.Replicas, totalTime.Seconds())

				return scaling.FunctionScaleResult{
					Error:     nil,
					Available: true,
					Found:     true,
					Duration:  totalTime,
					Replicas:  queryResponse.Replicas,
				}
			}

			time.Sleep(f.Config.FunctionPollInterval)
		}
	}

	return scaling.FunctionScaleResult{
		Error:     nil,
		Available: true,
		Found:     true,
		Duration:  time.Since(start),
		Replicas:  scaleInfo.Replicas,
	}
}
