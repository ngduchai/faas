package step

import (
	"errors"
	"net/http"
	"time"
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
	Resource         string `json:"Resource"`
	Parameters       string `json:"Parameters"`
	ResultPath       string `json:"ResultPath"`
	Retry            string `json:"Retry"`
	Catch            string `json:"Catch"`
	TimeoutSeconds   int    `json:"TimeoutSeconds"`
	HeartbeatSeconds int    `json:"HeartbeatSeconds"`

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
	TimeoutSeconds int               `json:"TimeoutSeconds"`
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
	FaasProcess string

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
}

// HandleRequest executes step function
func HandleRequest(config *Config, workflow *StateMachine, w http.ResponseWriter, r *http.Request, method string) {
	statusCode := http.StatusAccepted
	var err error = nil
	firstState := workflow.StartAt
	currentState, ok := workflow.States[firstState]
	for ok {
		switch currentState.Type {
		case "Task":
			// Perform computation
		default:
			err = errors.New("Unsupport state type: " + currentState.Type)
			break
		}
		if currentState.End {
			break
		}
		nextState := currentState.Next
		currentState = workflow.States[nextState]
	}
	w.WriteHeader(statusCode)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
}
