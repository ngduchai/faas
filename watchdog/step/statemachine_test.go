package step

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Test_StateMachine_single(t *testing.T) {

	body := "Hello, world"
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")

	comment := "Hello world function"
	startat := "singleTask"
	timeoutseconds := 10.0
	realtime := 1.0
	version := "1.0"
	stateType := "Task"
	resource := "helloworld"
	state := `{
		"Comment": "` + comment + `",
		"StartAt": "` + startat + `",
		"TimeoutSeconds": ` + strconv.FormatFloat(timeoutseconds, 'f', -1, 32) + `,
		"Realtime": ` + strconv.FormatFloat(realtime, 'f', -1, 32) + `,
		"Version": "` + version + `",
		"States": {
			"` + startat + `": {
				"Type": "` + stateType + `" ,
				"Resource": "` + resource + `",
				"TimeoutSeconds": ` + strconv.FormatFloat(timeoutseconds, 'f', -1, 32) + `,
				"End": true
			}
		}
	}`

	workflow, err := NewStateMachine([]byte(state))

	if err != nil {
		t.Errorf("StateMachine - error - Cannot parse JSON state machine: %s: %s", state, err.Error())
		t.Fail()
		return
	}
	if workflow.Comment != comment {
		t.Errorf("StateMachine - error Comment, want %s, got: %s", comment, workflow.Comment)
		t.Fail()
	}
	if workflow.StartAt != startat {
		t.Errorf("StateMachine - error StartAt, want %s, got: %s", startat, workflow.StartAt)
		t.Fail()
	}
	if workflow.TimeoutSeconds != timeoutseconds {
		t.Errorf("StateMachine - error TimeoutSeconds, want %f, got: %f", timeoutseconds, workflow.TimeoutSeconds)
		t.Fail()
	}
	if workflow.Realtime != realtime {
		t.Errorf("StateMachine - error Realtime, want %f, got: %f", realtime, workflow.Realtime)
		t.Fail()
	}
	if workflow.Version != version {
		t.Errorf("StateMachine - error Version, want %s, got: %s", version, workflow.Version)
		t.Fail()
	}
	if state, ok := workflow.States[startat]; !ok {
		t.Errorf("StateMachine - error States, dees not contain: %s", startat)
		t.Fail()
	} else {
		if state.Type != stateType {
			t.Errorf("StateMachine - error States Type, want %s, got: %s", stateType, state.Type)
			t.Fail()
		}
		if state.Resource != resource {
			t.Errorf("StateMachine - error States Resource, want %s, got: %s", resource, state.Resource)
			t.Fail()
		}
		if state.TimeoutSeconds != timeoutseconds {
			t.Errorf("StateMachine - error States TimeoutSeconds, want %f, got: %f", timeoutseconds, state.TimeoutSeconds)
			t.Fail()
		}
		if state.End != true {
			t.Errorf("StateMachine - error States End, want %v, got: %v", true, state.End)
			t.Fail()
		}
	}
}

func Test_StateMachine_sequence(t *testing.T) {

	body := "Hello, world"
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")

	comment := "Hello world function"
	startat := "startTask"
	endat := "endTask"
	timeoutseconds := 10.0
	realtime := 1.0
	version := "1.0"
	stateType := "Task"
	startresource := "Hello"
	endresource := "World"
	state := `{
		"Comment": "` + comment + `",
		"StartAt": "` + startat + `",
		"TimeoutSeconds": ` + strconv.FormatFloat(timeoutseconds, 'f', -1, 32) + `,
		"Realtime": ` + strconv.FormatFloat(realtime, 'f', -1, 32) + `,
		"Version": "` + version + `",
		"States": {
			"` + startat + `": {
				"Type": "` + stateType + `" ,
				"Resource": "` + startresource + `",
				"Next": "` + endat + `"
			},
			"` + endat + `": {
				"Type": "` + stateType + `" ,
				"Resource": "` + endresource + `",
				"End": true
			}
		}
	}`

	workflow, err := NewStateMachine([]byte(state))

	if err != nil {
		t.Errorf("StateMachine - error - Cannot parse JSON state machine: %s: %s", state, err.Error())
		t.Fail()
		return
	}
	if workflow.Comment != comment {
		t.Errorf("StateMachine - error Comment, want %s, got: %s", comment, workflow.Comment)
		t.Fail()
	}
	if workflow.StartAt != startat {
		t.Errorf("StateMachine - error StartAt, want %s, got: %s", startat, workflow.StartAt)
		t.Fail()
	}
	if workflow.TimeoutSeconds != timeoutseconds {
		t.Errorf("StateMachine - error TimeoutSeconds, want %f, got: %f", timeoutseconds, workflow.TimeoutSeconds)
		t.Fail()
	}
	if workflow.Realtime != realtime {
		t.Errorf("StateMachine - error Realtime, want %f, got: %f", realtime, workflow.Realtime)
		t.Fail()
	}
	if workflow.Version != version {
		t.Errorf("StateMachine - error Version, want %s, got: %s", version, workflow.Version)
		t.Fail()
	}
	if state, ok := workflow.States[startat]; !ok {
		t.Errorf("StateMachine - error States, does not contain: %s", startat)
		t.Fail()
	} else {
		if state.Type != stateType {
			t.Errorf("StateMachine - error Start State Type, want %s, got: %s", stateType, state.Type)
			t.Fail()
		}
		if state.Resource != startresource {
			t.Errorf("StateMachine - error Start State Resource, want %s, got: %s", startresource, state.Resource)
			t.Fail()
		}
		if state.Next != endat {
			t.Errorf("StateMachine - error Start State Next, want %s, got: %s", endat, state.Next)
			t.Fail()
		}
		if state, ok := workflow.States[state.Next]; !ok {
			t.Errorf("StateMachine - error States, dees not contain: %s", endat)
			t.Fail()
		} else {
			if state.Type != stateType {
				t.Errorf("StateMachine - error End State Type, want %s, got: %s", stateType, state.Type)
				t.Fail()
			}
			if state.Resource != endresource {
				t.Errorf("StateMachine - error End State Resource, want %s, got: %s", endresource, state.Resource)
				t.Fail()
			}
			if state.End != true {
				t.Errorf("StateMachine - error End State End, want %v, got: %v", true, state.End)
				t.Fail()
			}
		}
	}
}

func Test_handleRequest_single(t *testing.T) {

	startat := "singleTask"
	startResource := "cat"
	execTimeout := 4.0
	state := StateMachine{
		Comment:        "Hello world function",
		StartAt:        startat,
		TimeoutSeconds: execTimeout,
		Realtime:       1.0,
		Version:        "1.0",
		States:         map[string]*State{},
	}
	state.States[startat] = &State{
		Type:           "Task",
		Resource:       startResource,
		TimeoutSeconds: 2,
		End:            true,
	}

	body := `"Hello, world"`
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")
	w := httptest.NewRecorder()

	config := Config{
		CombineOutput: true,
		ExecTimeout:   time.Duration(execTimeout) * time.Second,
		Workflow:      state,
		FaasProcesses: map[string]string{},
		WriteDebug:    true,
		Name:          "singleTaskTest",
	}
	config.FaasProcesses[startResource] = "cat"
	method := http.MethodPost

	handleRequest(&config, w, r, method)
	response := w.Result()
	status := http.StatusOK
	if response.StatusCode != status {
		t.Errorf("handleRequest - error statusCode, want %d, got: %d", status, response.StatusCode)
		t.Fail()
	}
	resBody, _ := ioutil.ReadAll(response.Body)

	if string(resBody) != body {
		t.Errorf("handleRequest - error body, want %s, got: %s", body, string(resBody))
		t.Fail()
	}

}

func Test_handleRequest_timeout(t *testing.T) {

	startat := "singleTask"
	startResource := "sleep"
	execTimeout := 1.0
	state := StateMachine{
		Comment:        "Hello world function",
		StartAt:        startat,
		TimeoutSeconds: execTimeout,
		Realtime:       1.0,
		Version:        "1.0",
		States:         map[string]*State{},
	}
	state.States[startat] = &State{
		Type:           "Task",
		Resource:       startResource,
		TimeoutSeconds: 2,
		End:            true,
	}

	body := `"Hello, world"`
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")
	w := httptest.NewRecorder()

	config := Config{
		CombineOutput: true,
		ExecTimeout:   time.Duration(execTimeout) * time.Second,
		Workflow:      state,
		FaasProcesses: map[string]string{},
		WriteDebug:    true,
		Name:          "singleTaskTest",
	}
	config.FaasProcesses[startResource] = "sleep 2"
	method := http.MethodPost

	handleRequest(&config, w, r, method)
	response := w.Result()
	status := http.StatusOK
	if response.StatusCode == status {
		t.Errorf("handleRequest - error statusCode, want != %d, got: %d", status, response.StatusCode)
		t.Fail()
	}

	resBody, _ := ioutil.ReadAll(response.Body)
	if strings.Contains(string(resBody), "Timeout") {
		t.Errorf("handleRequest - error body, want containing 'Timeout', got: %s", string(resBody))
		t.Fail()
	}

}

func Test_handleRequest_sequence(t *testing.T) {

	startat := "hello"
	endat := "world"
	startResource := "tr"
	endResource := ""
	execTimeout := 4.0
	state := StateMachine{
		Comment:        "Hello world function",
		StartAt:        startat,
		TimeoutSeconds: execTimeout,
		Realtime:       1.0,
		Version:        "1.0",
		States:         map[string]*State{},
	}
	state.States[startat] = &State{
		Type:           "Task",
		Resource:       startResource,
		TimeoutSeconds: 2,
		Next:           endat,
	}
	state.States[endat] = &State{
		Type:           "Task",
		Resource:       endResource,
		TimeoutSeconds: 2,
		End:            true,
	}

	body := "TeSt, DatA"
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")
	w := httptest.NewRecorder()

	config := Config{
		CombineOutput: true,
		ExecTimeout:   time.Duration(execTimeout) * time.Second,
		Workflow:      state,
		FaasProcesses: map[string]string{},
		WriteDebug:    true,
		Name:          "singleTaskTest",
	}
	config.FaasProcesses[startResource] = "tr '[:upper:]' '[:lower:]'"
	config.FaasProcesses[endResource] = `tr -d ,`
	method := http.MethodPost

	handleRequest(&config, w, r, method)
	response := w.Result()
	status := http.StatusOK
	if response.StatusCode != status {
		t.Errorf("handleRequest - error statusCode, want %d, got: %d", status, response.StatusCode)
		t.Fail()
	}
	resBody, _ := ioutil.ReadAll(response.Body)

	tb := `test data`
	if string(resBody) != tb {
		t.Errorf("handleRequest - error body, want %s, got: %s", tb, string(resBody))
		t.Fail()
	}

}
