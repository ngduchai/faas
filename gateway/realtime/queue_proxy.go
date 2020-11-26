// Copyright (c) Alex Ellis 2017. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package realtime

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/ngduchai/faas/gateway/handlers"
	"github.com/ngduchai/faas/gateway/metrics"
	"github.com/ngduchai/faas/gateway/queue"
)

// MakeQueuedProxy accepts work onto a queue
func MakeQueuedProxy(metrics metrics.MetricOptions, wildcard bool, canQueueRequests queue.CanQueueRequests, pathTransformer handlers.URLPathTransformer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			defer r.Body.Close()
		}

		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)

			w.Write([]byte(err.Error()))
			return
		}

		vars := mux.Vars(r)
		name := vars["name"]

		callbackURLHeader := r.Header.Get("X-Callback-Url")
		var callbackURL *url.URL

		if len(callbackURLHeader) > 0 {
			urlVal, urlErr := url.Parse(callbackURLHeader)
			if urlErr != nil {
				w.WriteHeader(http.StatusBadRequest)

				w.Write([]byte(urlErr.Error()))
				return
			}

			callbackURL = urlVal
		}
		callid := r.Header.Get("X-Call-Id")
		if len(callid) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to create X-Call-Id"))
			return
		}

		// Check if the function can be invoked safely
		// scaler := scaling.GetScalerInstance()
		// invokeTime := time.Now()
		// added := false
		// scaleInfo, hit := scaler.Cache.Get(name)
		// if !hit {
		// 	scaleInfo, err = scaler.Config.ServiceQuery.GetReplicas(name)
		// 	if err == nil {
		// 		scaler.Cache.Set(name, scaleInfo)
		// 	} else {
		// 		w.WriteHeader(http.StatusBadRequest)
		// 		w.Write([]byte(err.Error()))
		// 		fmt.Printf("Cannot invoke function %s: %s\n", name, err.Error())
		// 		return
		// 	}

		// }
		// limit := scaleInfo.Realtime
		// if limit == 0.0 {
		// 	// Best effort invocation when no guarantee is enforced
		// 	added = true
		// } else {
		// 	_, _, added = scaler.Cache.UpdateInvocation(name, invokeTime)
		// }

		// if !added {
		// 	w.WriteHeader(http.StatusBadRequest)
		// 	w.Write([]byte("Too many requests"))
		// 	fmt.Printf("Cannot invoke function %s: Too many requests\n", name)
		// 	return
		// }

		err = AsyncInvoke(name, callid)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			log.Printf("Cannot invoke function %s: %s\n", name, err)
			return
		}

		req := &queue.Request{
			Function:    name,
			Body:        body,
			Method:      r.Method,
			QueryString: r.URL.RawQuery,
			Path:        pathTransformer.Transform(r),
			Header:      r.Header,
			Host:        r.Host,
			CallbackURL: callbackURL,
		}

		if err = canQueueRequests.Queue(req); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			fmt.Println(err)
			return
		}

		// if len(callid) == 0 {
		// 	w.WriteHeader(http.StatusInternalServerError)
		// 	w.Write([]byte("Unable to create X-Call-Id"))
		// 	return
		// } else {
		// 	scaler.BypassMap.Store(callid, true)
		// }

		w.WriteHeader(http.StatusAccepted)
	}
}
