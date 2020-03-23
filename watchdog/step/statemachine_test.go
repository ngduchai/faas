package step

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
)

func Test_StateMachine_single(t *testing.T) {

	body := "Hello, world"
	reader := bytes.NewReader([]byte(body))
	r, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	r.Header.Set("X-Source", "unit-test")

	comment := "Hello world function"
	startat := "singleTask"
	timeoutseconds := 10
	realtime := 1.0
	version := "1.0"
	stateType := "Task"
	resource := "helloworld"
	state := `{
		"Comment": "` + comment + `",
		"StartAt": "` + startat + `",
		"TimeoutSeconds": ` + strconv.FormatInt(int64(timeoutseconds), 10) + `,
		"Realtime": ` + strconv.FormatFloat(realtime, 'f', -1, 32) + `,
		"Version": "` + version + `",
		"States": {
			"` + startat + `": {
				"Type": "` + stateType + `" ,
				"Resource": "` + resource + `",
				"End": true
			}
		}
	}`

	workflow := StateMachine{}
	err := json.Unmarshal([]byte(state), &workflow)

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
		t.Errorf("StateMachine - error TimeoutSeconds, want %d, got: %d", timeoutseconds, workflow.TimeoutSeconds)
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
	timeoutseconds := 10
	realtime := 1.0
	version := "1.0"
	stateType := "Task"
	startresource := "Hello"
	endresource := "World"
	state := `{
		"Comment": "` + comment + `",
		"StartAt": "` + startat + `",
		"TimeoutSeconds": ` + strconv.FormatInt(int64(timeoutseconds), 10) + `,
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

	workflow := StateMachine{}
	err := json.Unmarshal([]byte(state), &workflow)

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
		t.Errorf("StateMachine - error TimeoutSeconds, want %d, got: %d", timeoutseconds, workflow.TimeoutSeconds)
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
