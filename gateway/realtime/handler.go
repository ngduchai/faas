package realtime

import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/openfaas/faas/gateway/handlers"
	"github.com/openfaas/faas/gateway/scaling"
	"github.com/openfaas/faas/gateway/types"
)

// Forward the deploy request to the backend.
func MakeRealtimeDeployHandler(
	ac AdmissionControl,
	proxy *types.HTTPClientReverseProxy,
	notifiers []handlers.HTTPNotifier,
	baseURLResolver handlers.BaseURLResolver,
	urlPathTransformer handlers.URLPathTransformer) http.HandlerFunc {

	writeRequestURI := false
	if _, exists := os.LookupEnv("write_request_uri"); exists {
		writeRequestURI = exists
	}

	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := baseURLResolver.Resolve(r)
		originalURL := r.URL.String()
		requestURL := urlPathTransformer.Transform(r)

		start := time.Now()

		statusCode, err := ac.Register(w, r, proxy.Client, baseURL, requestURL, proxy.Timeout, writeRequestURI)
		if err != nil {
			log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
		} else if statusCode < 200 || statusCode > 299 {
			log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, statusCode)
		}

		seconds := time.Since(start)

		for _, notifier := range notifiers {
			notifier.Notify(r.Method, requestURL, originalURL, statusCode, seconds)
		}
	}
}

// Forward the update request to the backend.
func MakeRealtimeUpdateHandler(
	ac AdmissionControl,
	proxy *types.HTTPClientReverseProxy,
	notifiers []handlers.HTTPNotifier,
	baseURLResolver handlers.BaseURLResolver,
	urlPathTransformer handlers.URLPathTransformer) http.HandlerFunc {

	writeRequestURI := false
	if _, exists := os.LookupEnv("write_request_uri"); exists {
		writeRequestURI = exists
	}

	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := baseURLResolver.Resolve(r)
		originalURL := r.URL.String()
		requestURL := urlPathTransformer.Transform(r)

		start := time.Now()

		statusCode, err := ac.Update(w, r, proxy.Client, baseURL, requestURL, proxy.Timeout, writeRequestURI)
		if err != nil {
			log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
		} else if statusCode < 200 || statusCode > 299 {
			log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, statusCode)
		}

		seconds := time.Since(start)

		for _, notifier := range notifiers {
			notifier.Notify(r.Method, requestURL, originalURL, statusCode, seconds)
		}
	}
}

// Forward the delete request to the backend.
func MakeRealtimeDeleteHandler(
	ac AdmissionControl,
	proxy *types.HTTPClientReverseProxy,
	notifiers []handlers.HTTPNotifier,
	baseURLResolver handlers.BaseURLResolver,
	urlPathTransformer handlers.URLPathTransformer) http.HandlerFunc {

	writeRequestURI := false
	if _, exists := os.LookupEnv("write_request_uri"); exists {
		writeRequestURI = exists
	}

	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := baseURLResolver.Resolve(r)
		originalURL := r.URL.String()
		requestURL := urlPathTransformer.Transform(r)

		start := time.Now()

		statusCode, err := ac.Unregister(w, r, proxy.Client, baseURL, requestURL, proxy.Timeout, writeRequestURI)
		if err != nil {
			log.Printf("error with upstream request to: %s, %s\n", requestURL, err.Error())
		} else if statusCode < 200 || statusCode > 299 {
			log.Printf("error with upstream request to: %s, Status code: %d\n", requestURL, statusCode)
		}

		seconds := time.Since(start)

		for _, notifier := range notifiers {
			notifier.Notify(r.Method, requestURL, originalURL, statusCode, seconds)
		}
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
