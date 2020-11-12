package realtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/ngduchai/faas/gateway/plugin"
	"github.com/ngduchai/faas/gateway/requests"
	"github.com/ngduchai/faas/gateway/scaling"
)

func Test_RequestRealtimeParams_with_realtime(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	// (*request.Labels)["realtime"] = "10"
	// (*request.Labels)["functionsize"] = "0.5"
	// (*request.Labels)["duration"] = "100"
	request.Realtime = 10
	request.Resources = &requests.FunctionResources{}
	request.Resources.CPU = "500m"
	request.Resources.Memory = "100M"
	request.Timeout = 100
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	request, error := rm.ParseRequest(hreq)

	if request.Service != "test" {
		t.Errorf("RequestRealtimeParams - functionName want: %s, got %s", "test", request.Service)
		t.Fail()
	}

	if request.Realtime != 10.0 {
		t.Errorf("RequestRealtimeParams - realtime want: %s, got %f", "10.0", request.Realtime)
		t.Fail()
	}
	cpu, memory, _ := rm.GetResourceQuantity(*request.Resources)
	if cpu != 500 {
		t.Errorf("RequestRealtimeParams - CPU want: %s, got %d", "500", cpu)
		t.Fail()
	}
	if memory != 100*1000000 {
		t.Errorf("RequestRealtimeParams - Memory want: %s, got %d", "100000000", memory)
		t.Fail()
	}
	if request.Timeout != 100 {
		t.Errorf("RequestRealtimeParams - duration: want: %s, got %d", "100", request.Timeout)
		t.Fail()
	}
	if error != nil {
		t.Errorf("RequestRealtimeParams - error want: %s, got %s", "nil", error.Error())
		t.Fail()
	}

}

func Test_RequestRealtimeParams_without_realtime(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = nil
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=2", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	request, error := rm.ParseRequest(hreq)

	if request.Service != "test" {
		t.Errorf("RequestRealtimeParams - functionName want: %s, got %s", "test", request.Service)
		t.Fail()
	}

	if request.Realtime != 0 {
		t.Errorf("RequestRealtimeParams - realtime want: %s, got %f", "0", request.Realtime)
		t.Fail()
	}
	if request.Resources != nil {
		t.Errorf("RequestRealtimeParams - size want: %s, got %s", "nil", "some unknown value")
		t.Fail()
	}
	if request.Timeout != 0 {
		t.Errorf("RequestRealtimeParams - duration: want: %s, got %d", "0", request.Timeout)
		t.Fail()
	}
	if error != nil {
		t.Errorf("RequestRealtimeParams - error want: %s, got %s", "nil", error.Error())
		t.Fail()
	}

}

func Test_RequestRealtimeParams_incorrect_message(t *testing.T) {
	reader := bytes.NewReader([]byte("Hello world"))
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=3", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	//functionName, realtime, size, duration, error := rm.RequestRealtimeParams(hreq)
	_, error := rm.ParseRequest(hreq)

	if error == nil {
		t.Errorf("RequestRealtimeParams - error want: %s, got %s", " != nil", "nil")
		t.Fail()
	}

}

// func Test_SetRealtimeParams_correct_params(t *testing.T) {
// 	request := requests.CreateFunctionRequest{}
// 	request.Service = "test"
// 	request.Labels = &map[string]string{}
// 	(*request.Labels)["realtime"] = "10"
// 	(*request.Labels)["cpu"] = "0.5"
// 	(*request.Labels)["memory"] = "1000"
// 	(*request.Labels)["duration"] = "100"
// 	body, _ := json.Marshal(request)
// 	reader := bytes.NewReader(body)
// 	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
// 	hreq.Header.Set("X-Source", "unit-test")

// 	rm := ResourceManager{}
// 	error := rm.SetRealtimeParams(hreq, 20, 100, 200, 1000)

// 	if error != nil {
// 		t.Errorf("SetRealtimeParams - error want: %s, got %s", "nil", error.Error())
// 		t.Fail()
// 	}
// 	body, _ = ioutil.ReadAll(hreq.Body)
// 	request = requests.CreateFunctionRequest{}
// 	error = json.Unmarshal(body, &request)
// 	if error != nil {
// 		t.Errorf("SetRealtimeParams - unable to unmarshal the updated request")
// 		t.Fail()
// 	}
// 	if request.Labels == nil {
// 		t.Errorf("SetRealtimeParams - empty Labels")
// 		t.Fail()
// 	}

// 	realtimeLabel := extractLabelRealValue((*request.Labels)["realtime"], 0)
// 	cpuLabel := extractLabelValue((*request.Labels)["cpu"], 500)
// 	memoryLabel := extractLabelValue((*request.Labels)["memory"], 1000)
// 	durationLabel := extractLabelValue((*request.Labels)["duration"], 100)
// 	realtime := request.Realtime
// 	cpu, _ := rm.GetCPUQuantity(request.Resources.CPU)
// 	memory, _ := rm.GetMemoryQuantity(request.Resources.Memory)
// 	duration := request.Timeout

// 	if realtime != 20 || realtimeLabel != 20 {
// 		t.Errorf("SetRealtimeParams - realtime want: %s, got %f %f", "20", realtime, realtimeLabel)
// 		t.Fail()
// 	}
// 	if cpu != 100 || cpuLabel != 100 {
// 		t.Errorf("SetRealtimeParams - size want: %s, got %d %d", "100", cpu, cpuLabel)
// 		t.Fail()
// 	}
// 	if memory != 200 || memoryLabel != 200 {
// 		t.Errorf("SetRealtimeParams - size want: %s, got %d %d", "0.1", memory, memoryLabel)
// 		t.Fail()
// 	}
// 	if duration != 1000 || durationLabel != 1000 {
// 		t.Errorf("SetRealtimeParams - duration want: %s, got %d %d", "120", duration, durationLabel)
// 		t.Fail()
// 	}
// }

// func Test_SetRealtimeParams_add_realtime_params(t *testing.T) {
// 	request := requests.CreateFunctionRequest{}
// 	request.Service = "test"
// 	request.Labels = nil
// 	body, _ := json.Marshal(request)
// 	reader := bytes.NewReader(body)
// 	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
// 	hreq.Header.Set("X-Source", "unit-test")

// 	rm := ResourceManager{}
// 	error := rm.SetRealtimeParams(hreq, 20, 100, 200, 1000)

// 	if error != nil {
// 		t.Errorf("SetRealtimeParams - error want: %s, got %s", "nil", error.Error())
// 		t.Fail()
// 	}
// 	body, _ = ioutil.ReadAll(hreq.Body)
// 	request = requests.CreateFunctionRequest{}
// 	error = json.Unmarshal(body, &request)
// 	if error != nil {
// 		t.Errorf("SetRealtimeParams - unable to unmarshal the updated request")
// 		t.Fail()
// 	}
// 	if request.Labels == nil {
// 		t.Errorf("SetRealtimeParams - empty Labels")
// 		t.Fail()
// 	}

// 	realtimeLabel := extractLabelRealValue((*request.Labels)["realtime"], 0)
// 	cpuLabel := extractLabelValue((*request.Labels)["cpu"], 200)
// 	memoryLabel := extractLabelValue((*request.Labels)["memory"], 2000000)
// 	durationLabel := extractLabelValue((*request.Labels)["duration"], 10000)
// 	realtime := request.Realtime
// 	cpu, _ := rm.GetCPUQuantity(request.Resources.CPU)
// 	memory, _ := rm.GetMemoryQuantity(request.Resources.Memory)
// 	duration := request.Timeout

// 	if realtime != 20 || realtimeLabel != 20 {
// 		t.Errorf("SetRealtimeParams - realtime want: %s, got %f %f", "20", realtime, realtimeLabel)
// 		t.Fail()
// 	}
// 	if cpu != 100 || cpuLabel != 100 {
// 		t.Errorf("SetRealtimeParams - size want: %s, got %d %d", "0.1", cpu, cpuLabel)
// 		t.Fail()
// 	}
// 	if memory != 200 || memoryLabel != 200 {
// 		t.Errorf("SetRealtimeParams - size want: %s, got %d %d", "0.1", memory, memoryLabel)
// 		t.Fail()
// 	}
// 	if duration != 1000 || durationLabel != 1000 {
// 		t.Errorf("SetRealtimeParams - duration want: %s, got %d %d", "120", duration, durationLabel)
// 		t.Fail()
// 	}
// }

type TestServiceQuery struct {
	Info map[string]*scaling.ServiceQueryResponse
	plugin.ExternalServiceQuery
}

func (sq TestServiceQuery) GetReplicas(serviceName string) (scaling.ServiceQueryResponse, error) {
	if f, ok := sq.Info[serviceName]; ok {
		avail := sq.Info[serviceName].AvailableReplicas
		if avail < 10 {
			sq.Info[serviceName].AvailableReplicas++
		}
		//fmt.Printf("Access service %d\n", sq.Info[serviceName].AvailableReplicas)
		return *f, nil
	} else {
		return scaling.ServiceQueryResponse{}, errors.New("function not found")
	}
}

func (sq TestServiceQuery) SetReplicas(serviceName string, count uint64) error {
	if _, ok := sq.Info[serviceName]; ok {
		sq.Info[serviceName].Replicas = count
		return nil
	} else {
		return errors.New("Function not found")
	}
}

func Test_DeploymentRealtimeParams(t *testing.T) {
	f := scaling.GetScalerInstance()
	replicas := uint64(10)
	maxReplicas := uint64(10)
	minReplicas := uint64(0)
	scalingFactor := uint64(2)
	availableReplicas := uint64(2)
	realTime := float64(1)
	cpu := int64(100)
	memory := int64(1000)
	functionDuration := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          realTime,
		CPU:               cpu,
		Memory:            memory,
		Duration:          functionDuration,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}

	params, err := rm.GetDeploymentParams("noinfo")
	if err == nil {
		t.Errorf("DeploymentRealtimeParams - get noinfo, want: %s, got %s", "function not found error", "nil")
		t.Fail()
	}

	params, err = rm.GetDeploymentParams("test")
	if err != nil {
		t.Errorf("DeploymentRealtimeParams - get test error, want: %s, got %s", "nil", err.Error())
		t.Fail()
	}
	if params.Realtime != realTime {
		t.Errorf("DeploymentRealtimeParams - get test realtime, want: %f, got %f", realTime, params.Realtime)
		t.Fail()
	}
	if params.CPU != cpu {
		t.Errorf("DeploymentRealtimeParams - get test size, want: %d, got %d", cpu, params.CPU)
		t.Fail()
	}
	if memory != params.Memory {
		t.Errorf("DeploymentRealtimeParams - get test size, want: %d, got %d", memory, params.Memory)
		t.Fail()
	}
	if params.Duration != functionDuration {
		t.Errorf("DeploymentRealtimeParams - get test duration, want: %d, got %d", functionDuration, params.Duration)
		t.Fail()
	}
}

func Test_GetAvailReplicas(t *testing.T) {
	f := scaling.GetScalerInstance()
	replicas := uint64(20)
	maxReplicas := uint64(40)
	minReplicas := uint64(0)
	scalingFactor := uint64(2)
	availableReplicas := uint64(10)
	realTime := float64(1)
	cpu := int64(100)
	memory := int64(1000)
	functionDuration := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          realTime,
		CPU:               cpu,
		Memory:            memory,
		Duration:          functionDuration,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}

	avail, error := rm.GetAvailReplicas("noinfo")
	if error == nil {
		t.Errorf("GetAvailReplicas - get noinfo, want: %s, got %s", "non-nil", "nil")
		t.Fail()
	}

	serviceQuery.Info["test"].AvailableReplicas = availableReplicas
	f.Config.ServiceQuery = serviceQuery
	avail, error = rm.GetAvailReplicas("test")
	if error != nil {
		t.Errorf("GetAvailReplicas - get test, want: %s, got %s", "nil", error.Error())
		t.Fail()
	}
	if avail != availableReplicas {
		t.Errorf("GetAvailReplicas - get test, want: %d, got %d", availableReplicas, avail)
		t.Fail()
	}
}

func Test_WaitforAvailReplicas(t *testing.T) {
	f := scaling.GetScalerInstance()
	replicas := uint64(20)
	maxReplicas := uint64(40)
	minReplicas := uint64(0)
	scalingFactor := uint64(2)
	availableReplicas := uint64(2)
	realTime := float64(1)
	cpu := int64(100)
	memory := int64(1000)
	functionDuration := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          realTime,
		CPU:               cpu,
		Memory:            memory,
		Duration:          functionDuration,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}
	interval := 1
	retry := uint64(5)

	ready := rm.WaitForAvailReplicas("noinfo", replicas, retry, interval)
	if ready {
		t.Errorf("WaitForAvailReplicas - get noinfo, want: %s, got %s", "false", "true")
		t.Fail()
	}

	//retry = replicas / 4
	serviceQuery.Info["test"].AvailableReplicas = 0
	f.Config.ServiceQuery = serviceQuery
	ready = rm.WaitForAvailReplicas("test", replicas, retry, interval)
	serviceQuery = f.Config.ServiceQuery.(TestServiceQuery)
	if ready {
		t.Errorf("WaitForAvailReplicas - get test error, want: %s, got %s", "false", "true")
		t.Fail()
	}
	if serviceQuery.Info["test"].AvailableReplicas != 10 {
		t.Errorf("WaitForAvailReplicas - get test count, want: %d, got %d", retry, serviceQuery.Info["test"].AvailableReplicas)
		t.Fail()
	}

	//retry = replicas * 2
	replicas = uint64(7)
	serviceQuery.Info["test"].AvailableReplicas = 0
	serviceQuery.Info["test"].Replicas = replicas
	serviceQuery = serviceQuery
	f.Config.ServiceQuery = serviceQuery
	ready = rm.WaitForAvailReplicas("test", replicas, retry, interval)
	serviceQuery = f.Config.ServiceQuery.(TestServiceQuery)
	if !ready {
		t.Errorf("WaitForAvailReplicas - get test error, want: %s, got %s", "true", "false")
		t.Fail()
	}
	if serviceQuery.Info["test"].AvailableReplicas < replicas {
		t.Errorf("WaitForAvailReplicas - get test available replicas, want: %d, got %d", replicas, serviceQuery.Info["test"].AvailableReplicas)
		t.Fail()
	}

}

func Test_Scale(t *testing.T) {
	f := scaling.GetScalerInstance()
	replicas := uint64(10)
	maxReplicas := uint64(10)
	minReplicas := uint64(0)
	scalingFactor := uint64(2)
	availableReplicas := uint64(2)
	realTime := float64(1)
	cpu := int64(100)
	memory := int64(1000)
	functionDuration := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          realTime,
		CPU:               cpu,
		Memory:            memory,
		Duration:          functionDuration,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}
	numReplicas := 2 * replicas

	error := rm.Scale("noinfo", 2*replicas)
	if error == nil {
		t.Errorf("Scale - get noinfo, want: %s, got %s", "function not found", "nil")
		t.Fail()
	}

	serviceQuery.Info["test"].AvailableReplicas = 0
	f.Config.ServiceQuery = serviceQuery
	numReplicas = replicas / 4
	error = rm.Scale("test", numReplicas)
	serviceQuery = f.Config.ServiceQuery.(TestServiceQuery)
	if error != nil {
		t.Errorf("Scale - get test error, want: %s, got %s", "nil", error.Error())
		t.Fail()
	}
	if serviceQuery.Info["test"].Replicas < numReplicas {
		t.Errorf("Scale - get test available replicas, want: %d, got %d", numReplicas, serviceQuery.Info["test"].AvailableReplicas)
		t.Fail()
	}

	serviceQuery.Info["test"].AvailableReplicas = 0
	f.Config.ServiceQuery = serviceQuery
	numReplicas = replicas * 2
	error = rm.Scale("test", numReplicas)
	serviceQuery = f.Config.ServiceQuery.(TestServiceQuery)
	if error != nil {
		t.Errorf("Scale - get test error, want: %s, got %s", "nil", error.Error())
		t.Fail()
	}
	if serviceQuery.Info["test"].Replicas < numReplicas {
		t.Errorf("Scale - get test available replicas, want: %d, got %d", numReplicas, serviceQuery.Info["test"].AvailableReplicas)
		t.Fail()
	}
}

/*
type SuccessHttpClient struct {
	http.Client
}

func (c *SuccessHttpClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Status:     "200 Accepted",
		StatusCode: http.StatusAccepted,
	}, nil
}

type FailHttpClient struct {
	http.Client
}

func (c *FailHttpClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Status:     "502 Accepted",
		StatusCode: http.StatusInternalServerError,
	}, errors.New("Error occurs")
}

func Test_CreateImage(t *testing.T) {
	rm := ResourceManager{}
	res, err := rm.CreateImage(r, proxyClient, baseURL, requestURL, 1*time.Seconds(), true)

}
*/
