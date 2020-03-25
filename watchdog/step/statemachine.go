package step

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/ngduchai/faas/watchdog/types"
)

// ChoiceRule represents the operator used for making
// choices by a Choice state
type ChoiceRule struct {
	Variable string `json:"Variable"`
	Next     string `json:"Next"`
}

// Branch represents states to execute in parallel
type Branch struct {
	States  []State `json:"States"`
	StartAt string  `json:"StartAt"`
}

// State represents a single state describe in Step function
// state machine
type State struct {
	/* Common parameters */
	Type       string `json:"Type"`
	Next       string `json:"Next"`
	End        bool   `json:"End"`
	Comment    string `json:"Comment"`
	InputPath  string `json:"InputPath"`
	OutputPath string `json:"OutputPath"`

	/* Task parameters */
	Resource         string  `json:"Resource"`
	Parameters       string  `json:"Parameters"`
	ResultPath       string  `json:"ResultPath"`
	Retry            string  `json:"Retry"`
	Catch            string  `json:"Catch"`
	TimeoutSeconds   float64 `json:"TimeoutSeconds"`
	HeartbeatSeconds int     `json:"HeartbeatSeconds"`

	/* Pass parameters, shares ResultPath and Parameters with other states */
	Result string `json:"Result"`

	/* Choice parameters */
	Choices []ChoiceRule `json:"Choice"`
	Default string       `json:"Default"`

	/* Wait parameters */
	Seconds       int    `json:"Seconds"`
	Timestamp     string `json:"Timestamp"`
	SecondsPath   string `json:"SecondsPath"`
	TimestampPath string `json:"TimestampPath"`

	/* Fail parameters */
	Cause string `json:"Cause"`
	Error string `json:"Error"`

	/* Success parameters */

	/* Parallel parameters */
	Branches []Branch `json:"Branches"`

	/* Map parameters */
	Iterator       StateMachine `json:"Iterator"`
	ItemsPath      string       `json:"ItemsPath"`
	MaxConcurrency int          `json:"MaxConcurrency"`
}

// StateMachine represent the execution flow and
// information needed to execute a step function
type StateMachine struct {
	Comment        string            `json:"Comment"`
	StartAt        string            `json:"StartAt"`
	TimeoutSeconds float64           `json:"TimeoutSeconds"`
	Realtime       float64           `json:"Realtime"`
	Version        string            `json:"Version"`
	States         map[string]*State `json:"States"`
}

// Config for the process.
type Config struct {

	// HTTP read timeout
	ReadTimeout time.Duration

	// HTTP write timeout
	WriteTimeout time.Duration

	// faasProcess is the process to exec
	FaasProcesses map[string]string

	// duration until the faasProcess will be killed
	ExecTimeout time.Duration

	// writeDebug write console stdout statements to the container
	WriteDebug bool

	// marshal header and body via JSON
	MarshalRequest bool

	// cgiHeaders will make environmental variables available with all the HTTP headers.
	CgiHeaders bool

	// prints out all incoming and out-going HTTP headers
	DebugHeaders bool

	// Don't write a lock file to /tmp/
	SuppressLock bool

	// contentType forces a specific pre-defined value for all responses
	ContentType string

	// port for HTTP server
	Port int

	// combineOutput combines stderr and stdout in response
	CombineOutput bool

	// metricsPort is the HTTP port to serve metrics on
	MetricsPort int

	// Workflow is a StateMachine used for handling invocation requests
	Workflow StateMachine

	// Name is the name of the workflow visible by
	// other users
	Name string
}

func doTask(config *Config, state *State, timeleft float64, in []byte) ([]byte, error) {
	taskInfo := "doTask " + state.Resource + " "
	startTime := time.Now()
	faasProcess, ok := config.FaasProcesses[state.Resource]
	if !ok {
		return nil, errors.New(taskInfo + "Function not found")
	}
	parts := strings.Split(faasProcess, " ")

	log.Println(taskInfo + "Forking new process: " + faasProcess)

	targetCmd := exec.Command(parts[0], parts[1:]...)

	targetCmd.Env = os.Environ()

	writer, _ := targetCmd.StdinPipe()

	var out []byte = nil
	var err error = nil

	var wg sync.WaitGroup

	wgCount := 2

	// Probably need to convert the input into
	// a form the function process understand

	wg.Add(wgCount)

	var timer *time.Timer

	if timeleft > state.TimeoutSeconds {
		timeleft = state.TimeoutSeconds
	}
	if timeleft > 0 {
		timeout := time.Duration(timeleft * float64(time.Second))
		timer = time.AfterFunc(timeout, func() {
			log.Printf(taskInfo + "Terminating " + state.Resource)
			if targetCmd != nil && targetCmd.Process != nil {
				err = errors.New("Timeout, killed")
				val := targetCmd.Process.Kill()
				if val != nil {
					log.Printf(taskInfo+"Killed process: %s - error %s\n", faasProcess, val.Error())
				}
			}
		})
	}

	// Write to pipe in separate go-routine to provent blocking
	go func() {
		defer wg.Done()
		writer.Write(in)
		writer.Close()
	}()

	if config.CombineOutput {
		// Read the output from stdout/stderr and combine into one variable for output.
		go func() {
			defer wg.Done()

			out, err = targetCmd.CombinedOutput()
		}()
	} else {
		go func() {
			var b bytes.Buffer
			targetCmd.Stderr = &b

			defer wg.Done()

			out, err = targetCmd.Output()
			if b.Len() > 0 {
				log.Printf(taskInfo+"stderr: %s,", b.Bytes())
			}
			b.Reset()
		}()
	}

	wg.Wait()
	if timer != nil {
		timer.Stop()
	}

	if config.WriteDebug == true {
		if err != nil {
			log.Printf(taskInfo+"Success=%t, Out='%s', Error='%s'\n", targetCmd.ProcessState.Success(), out, err.Error())
		} else {
			log.Printf(taskInfo+"Success=%t, Out='%s', Error=''\n", targetCmd.ProcessState.Success(), out)
		}
	}

	bytesWritten := fmt.Sprintf("wrote %d bytes", len(out))

	execDuration := time.Since(startTime).Seconds()
	if len(bytesWritten) > 0 {
		log.Printf(taskInfo+"%s - Duration : %f seconds", bytesWritten, execDuration)
	} else {
		log.Printf(taskInfo+"Duration: %f seconds", execDuration)
	}
	return out, err
}

// NewStateMachine returns a StateMachine based on
// state machine description written in JSON format
func NewStateMachine(workflow []byte) (StateMachine, error) {
	statemachine := StateMachine{}
	err := json.Unmarshal(workflow, &statemachine)
	return statemachine, err
}

// HandleRequest executes step function
func handleRequest(config *Config, w http.ResponseWriter, r *http.Request, method string) {
	startTime := time.Now()
	log.Println("Start a workflow " + config.Name)

	statusCode := http.StatusOK
	var pipeOut []byte
	workflow := config.Workflow
	firstState := workflow.StartAt

	// Read request body
	pipeIn, err := ioutil.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
	}
	if config.MarshalRequest {
		pipeIn, err = types.MarshalRequest(pipeIn, &r.Header)
	}
	currentState, running := workflow.States[firstState]

	// Loop over the state machine until reach an end
	// state or error occurs
	for running == true && err == nil {
		// Determine the current state type to perform
		// appropriate tasks
		timeleft := float64(config.ExecTimeout-time.Since(startTime)) / float64(time.Second)
		// First, ensure that processing the sate is
		// strictied within allowed time duration
		if timeleft < 0 {
			err = errors.New("Timeout, workflow is terminated")
			break
		}
		switch currentState.Type {
		case "Task":
			// Perform computation
			pipeOut, err = doTask(config, currentState, timeleft, pipeIn)
			if err != nil {
				statusCode = http.StatusBadRequest
			}
		default:
			statusCode = http.StatusNotImplemented
			err = errors.New("Unsupported state type: " + currentState.Type)
			break
		}

		// Moving to the next state
		if currentState.End == false {
			nextState := currentState.Next
			currentState, running = workflow.States[nextState]
			pipeIn = pipeOut
		} else {
			running = false
		}
	}
	w.WriteHeader(statusCode)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(pipeOut)
	}
	execDuration := time.Since(startTime).Seconds()
	log.Printf("Finish %s workflow execution [%d] - Duration: %f\n", config.Name, statusCode, execDuration)
	if config.WriteDebug {
		if err == nil {
			log.Printf("%s execution - Out='%s' Error=''", config.Name, pipeOut)
		} else {
			log.Printf("%s execution - Out='%s' Error='%s'", config.Name, pipeOut, err.Error())
		}
	}
}

func getAdditionalEnvs(config *Config, r *http.Request, method string) []string {
	var envs []string

	if config.CgiHeaders {
		envs = os.Environ()

		for k, v := range r.Header {
			kv := fmt.Sprintf("Http_%s=%s", strings.Replace(k, "-", "_", -1), v[0])
			envs = append(envs, kv)
		}

		envs = append(envs, fmt.Sprintf("Http_Method=%s", method))
		// Deprecation notice: Http_ContentLength will be deprecated
		envs = append(envs, fmt.Sprintf("Http_ContentLength=%d", r.ContentLength))
		envs = append(envs, fmt.Sprintf("Http_Content_Length=%d", r.ContentLength))

		if config.WriteDebug {
			log.Println("Query ", r.URL.RawQuery)
		}

		if len(r.URL.RawQuery) > 0 {
			envs = append(envs, fmt.Sprintf("Http_Query=%s", r.URL.RawQuery))
		}

		if config.WriteDebug {
			log.Println("Path ", r.URL.Path)
		}

		if len(r.URL.Path) > 0 {
			envs = append(envs, fmt.Sprintf("Http_Path=%s", r.URL.Path))
		}

		if len(r.Host) > 0 {
			envs = append(envs, fmt.Sprintf("Http_Host=%s", r.Host))
		}

	}

	return envs
}

// MakeRequestHandler return a handle to a step
// function request.
func MakeRequestHandler(config *Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodGet:
			handleRequest(config, w, r, r.Method)
			break
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)

		}
	}
}
