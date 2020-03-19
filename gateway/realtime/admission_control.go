package realtime

import (
	"net/http"
	"time"
)

// AdmissionControl admission control interface for function deployment,
// update and deletion
type AdmissionControl interface {
	Register(
		w http.ResponseWriter,
		r *http.Request,
		proxyClient *http.Client,
		baseURL string,
		requestURL string,
		timeout time.Duration,
		writeRequestURI bool) (int, error)
	Update(
		w http.ResponseWriter,
		r *http.Request,
		proxyClient *http.Client,
		baseURL string,
		requestURL string,
		timeout time.Duration,
		writeRequestURI bool) (int, error)
	Unregister(
		w http.ResponseWriter,
		r *http.Request,
		proxyClient *http.Client,
		baseURL string,
		requestURL string,
		timeout time.Duration,
		writeRequestURI bool) (int, error)
}
