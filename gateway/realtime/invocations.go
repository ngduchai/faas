package realtime

import (
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ngduchai/faas/gateway/requests"
)

// Invocation holds information of pending requests
type Invocation struct {
	next http.HandlerFunc
	w    http.ResponseWriter
	r    *http.Request
	cond *sync.Cond
}

var functionHandlers sync.Map

// InvocationHandler process new invocations
type InvocationHandler struct {
	// Channel for pending invocations
	SyncInvs  chan Invocation
	AsyncInvs chan Invocation
	Realtime  float64
	AsyncWait sync.Map
	//FreeAsync int32
	Timing     *time.Ticker
	Stop       chan bool
	Update     chan bool
	BufferSize int
	Idle       chan bool
}

// SetFunctionHandler create handler for a new function or update an existing one
func SetFunctionHandler(f requests.CreateFunctionRequest) {
	functionName := f.Service
	if entry, ok := functionHandlers.Load(functionName); ok {
		log.Printf("Handler %s already exists, update\n", functionName)
		// The function handler is already exists, we just need to adjust
		// its parameters
		handler := entry.(*InvocationHandler)
		handler.Realtime = f.Realtime
		if handler.Realtime > 0 {
			if handler.Timing != nil {
				handler.Timing.Stop()
			}
			handler.Timing = time.NewTicker(time.Duration(1 / f.Realtime * 1000000000))
			handler.Update <- true
		}
	} else {
		log.Printf("Add new handler entry for %s\n", functionName)
		buffersize := 200
		handler := InvocationHandler{
			BufferSize: buffersize,
			SyncInvs:   make(chan Invocation, buffersize),
			AsyncInvs:  make(chan Invocation, buffersize),
			//FreeAsync: 1000,
			Realtime: f.Realtime,
			Stop:     make(chan bool),
			Update:   make(chan bool),
			Idle:     make(chan bool),
		}
		if f.Realtime > 0 {
			handler.Timing = time.NewTicker(time.Duration(1 / f.Realtime * 1000000000))
		} else {
			// Set timming but stop immediately since we dont need periodic control over invocations
			handler.Timing = time.NewTicker(time.Duration(1 * 1000000000))
			handler.Timing.Stop()
		}
		functionHandlers.Store(functionName, &handler)
		log.Printf("Starting handler for %s\n", functionName)
		go func() {
			timeout := 10000 * time.Second
			if handler.Realtime > 0 {
				timeout = time.Duration(1 / f.Realtime * 1000000000000)
			}
			checktime := time.Now()
			for {
				select {
				case <-handler.Stop:
					handler.Timing.Stop()
					log.Printf("Handler for %s stops", functionName)
					return
				case <-handler.Update:
					log.Printf("Update handler timing %s\n", functionName)
				case <-handler.Timing.C:
					// log.Printf("Looking for new invocation %s\n", functionName)
					select {
					case asyncInvocation, ok := <-handler.AsyncInvs:
						if !ok {
							log.Println("Invocation channels are closed, stop scheduling invocations")
							return
						} else {
							log.Printf("Executing async %s after %d\n", functionName, time.Since(checktime)/time.Millisecond)
							checktime = time.Now()
							go func() {
								asyncInvocation.next(asyncInvocation.w, asyncInvocation.r)
								asyncInvocation.cond.Signal()
							}()
						}
					case syncInvocation, ok := <-handler.SyncInvs:
						if !ok {
							log.Println("Invocation channels are closed, stop scheduling invocations")
							return
						} else {
							// log.Printf("Executing sync %s", functionName)
							go func() {
								syncInvocation.next(syncInvocation.w, syncInvocation.r)
								syncInvocation.cond.Signal()
							}()
						}
					case <-handler.Stop:
						handler.Timing.Stop()
						log.Printf("Handler for %s stops", functionName)
						return
					case <-handler.Update:
						log.Printf("Update handler timing %s\n", functionName)
					case <-time.After(timeout):
						handler.Timing = time.NewTicker(time.Duration(1 / f.Realtime * 1000000000))
						//default:
						// log.Printf("Invocation queue of %s is empty at timer tick, stop timming", functionName)
						// handler.Timing.Stop()
						// log.Printf("Enter idle state, waiting for invocation %s", functionName)
						// select {
						// case asyncInvocation, ok := <-handler.AsyncInvs:
						// 	if !ok {
						// 		log.Println("Invocation channels are closed, stop scheduling invocations")
						// 		return
						// 	} else {
						// 		// log.Printf("Executing sync %s", functionName)
						// 		go func() {
						// 			asyncInvocation.next(asyncInvocation.w, asyncInvocation.r)
						// 			asyncInvocation.cond.Signal()
						// 		}()
						// 	}
						// case syncInvocation, ok := <-handler.SyncInvs:
						// 	if !ok {
						// 		log.Println("Invocation channels are closed, stop scheduling invocations")
						// 		return
						// 	} else {
						// 		// log.Printf("Executing async %s", functionName)
						// 		go func() {
						// 			syncInvocation.next(syncInvocation.w, syncInvocation.r)
						// 			syncInvocation.cond.Signal()
						// 		}()
						// 	}
						// }
						// // Restart timmer if the function is realtime
						// if f.Realtime > 0 {
						// 	log.Printf("Invocation to %s, restart timmer\n", functionName)
						// 	handler.Timing = time.NewTicker(time.Duration(1 / f.Realtime * 1000000000))
						// }
					}
					if len(handler.Timing.C) > 0 && f.Realtime > 0 {
						handler.Timing = time.NewTicker(time.Duration(1 / f.Realtime * 1000000000))
					}
				}
			}
		}()
	}
}

func RemoveFunctionHandler(functionName string) {
	entry, ok := functionHandlers.Load(functionName)
	if !ok {
		// no handler exist, forward for further processing
		return
	}
	handler := entry.(*InvocationHandler)
	handler.Stop <- true
	handler.Timing.Stop()
	close(handler.SyncInvs)
	close(handler.AsyncInvs)
	functionHandlers.Delete(functionName)
}

func AsyncInvoke(functionName string, id string) error {
	entry, ok := functionHandlers.Load(functionName)
	if !ok {
		// no handler exist, forward for further processing
		return errors.New("Function handler not found")
	}
	handler := entry.(*InvocationHandler)
	if handler.Realtime > 0 {
		//avail := atomic.AddInt32(&handler.FreeAsync, -1)
		avail := handler.BufferSize - len(handler.AsyncInvs)
		// log.Printf("Available slot for %s: %d\n", functionName, avail)
		if avail <= 0 {
			// atomic.AddInt32(&handler.FreeAsync, 1)
			return errors.New("Too many invocations")
		} else {
			// log.Printf("Adding new function for %s\n", functionName)
			handler.AsyncWait.Store(id, time.Now())
			return nil
		}
	}
	return nil
}

// Invoke execute a deployed function
func Invoke(next http.HandlerFunc, w http.ResponseWriter, r *http.Request) error {
	// Infer function name from the url
	originalURL := r.URL.String()
	originalURL = strings.TrimSuffix(originalURL, "/")
	tokens := strings.Split(originalURL, "/")
	functionName := tokens[len(tokens)-1]
	// log.Printf("Invoke function: %s\n", functionName)

	start := time.Now()

	entry, ok := functionHandlers.Load(functionName)
	if !ok {
		// no handler exist, forward for further processing
		log.Printf("Function handler not found %s\n", functionName)
		next(w, r)
		return errors.New("Function handler not found")
	}
	handler := entry.(*InvocationHandler)
	if handler.Realtime == 0 {
		// Best-effort serverless, go forward
		// log.Printf("Realtime = 0, run as best-effort function %s\n", functionName)
		next(w, r)
	} else {
		// Real-time serverless, go through real-time scheduling
		invocation := Invocation{
			next: next,
			w:    w,
			r:    r,
			cond: sync.NewCond(&sync.Mutex{}),
		}
		// Check if the invocation is an asynchronous call that has been added
		callid := r.Header.Get("X-Call-Id")
		if _, added := handler.AsyncWait.Load(callid); added {
			handler.AsyncWait.Delete(callid)
			// atomic.AddInt32(&handler.FreeAsync, 1)
			select {
			case handler.AsyncInvs <- invocation:
				// Successfully add new invocation to the channel for real-time scheduling
				// Wait until the execution success
				log.Printf("Add invocation to async queue %s: %d\n", functionName, time.Since(start)/time.Millisecond)
				invocation.cond.L.Lock()
				invocation.cond.Wait()
				invocation.cond.L.Unlock()
				log.Printf("Execute invocation %s: %d ms\n", functionName, time.Since(start)/time.Millisecond)
				return nil
			default:
				// Unable to add new invocation because the buffer is full
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte("Too many requests"))
				log.Printf("Cannot invoke function asynchronously %s: Too many requests\n", functionName)
			}
		} else {
			select {
			case handler.SyncInvs <- invocation:
				// Successfully add new invocation to the channel for real-time scheduling
				// Wait until the execution success
				log.Printf("Add invocation to sync queue %s\n", functionName)
				invocation.cond.L.Lock()
				invocation.cond.Wait()
				invocation.cond.L.Unlock()
				return nil
			default:
				// Unable to add new invocation because the buffer is full
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte("Too many requests"))
				log.Printf("Cannot invoke function %s: Too many requests\n", functionName)
			}
		}
	}
	return nil

}
