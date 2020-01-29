package realtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/openfaas/faas/gateway/plugin"
	"github.com/openfaas/faas/gateway/requests"
	"github.com/openfaas/faas/gateway/scaling"
)

func Test_RequestRealtimeParams_with_realtime(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	(*request.Labels)["realtime"] = "10"
	(*request.Labels)["functionsize"] = "0.5"
	(*request.Labels)["duration"] = "100"
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	functionName, realtime, size, duration, error := rm.RequestRealtimeParams(hreq)

	if functionName != "test" {
		t.Errorf("RequestRealtimeParams - functionName want: %s, got %s", "test", functionName)
		t.Fail()
	}

	if realtime != 10.0 {
		t.Errorf("RequestRealtimeParams - realtime want: %s, got %f", "10.0", realtime)
		t.Fail()
	}
	if size != 0.5 {
		t.Errorf("RequestRealtimeParams - size want: %s, got %f", "0.5", size)
		t.Fail()
	}
	if duration != 100 {
		t.Errorf("RequestRealtimeParams - duration: want: %s, got %d", "100", duration)
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
	functionName, realtime, size, duration, error := rm.RequestRealtimeParams(hreq)

	if functionName != "test" {
		t.Errorf("RequestRealtimeParams - functionName want: %s, got %s", "test", functionName)
		t.Fail()
	}

	if realtime != 0 {
		t.Errorf("RequestRealtimeParams - realtime want: %s, got %f", "0", realtime)
		t.Fail()
	}
	if size != 1.0 {
		t.Errorf("RequestRealtimeParams - size want: %s, got %f", "1.0", size)
		t.Fail()
	}
	if duration != 60 {
		t.Errorf("RequestRealtimeParams - duration: want: %s, got %d", "60", duration)
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
	_, _, _, _, error := rm.RequestRealtimeParams(hreq)

	if error == nil {
		t.Errorf("RequestRealtimeParams - error want: %s, got %s", " != nil", "nil")
		t.Fail()
	}

}

func Test_SetRealtimeParams_correct_params(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	(*request.Labels)["realtime"] = "10"
	(*request.Labels)["functionsize"] = "0.5"
	(*request.Labels)["duration"] = "100"
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	error := rm.SetRealtimeParams(hreq, 20, 0.1, 120)

	if error != nil {
		t.Errorf("SetRealtimeParams - error want: %s, got %s", "nil", error.Error())
		t.Fail()
	}
	body, _ = ioutil.ReadAll(hreq.Body)
	request = requests.CreateFunctionRequest{}
	error = json.Unmarshal(body, &request)
	if error != nil {
		t.Errorf("SetRealtimeParams - unable to unmarshal the updated request")
		t.Fail()
	}
	if request.Labels == nil {
		t.Errorf("SetRealtimeParams - empty Labels")
		t.Fail()
	}

	realtime := extractLabelRealValue((*request.Labels)["realtime"], float64(0))
	size := extractLabelRealValue((*request.Labels)["functionsize"], float64(1.0))
	duration := extractLabelValue((*request.Labels)["duration"], uint64(60))
	if realtime != 20 {
		t.Errorf("SetRealtimeParams - realtime want: %s, got %f", "20", realtime)
		t.Fail()
	}
	if size != 0.1 {
		t.Errorf("SetRealtimeParams - size want: %s, got %f", "0.1", size)
		t.Fail()
	}
	if duration != 120 {
		t.Errorf("SetRealtimeParams - duration want: %s, got %d", "120", duration)
		t.Fail()
	}
}

func Test_SetRealtimeParams_add_realtime_params(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = nil
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	error := rm.SetRealtimeParams(hreq, 20, 0.1, 120)

	if error != nil {
		t.Errorf("SetRealtimeParams - error want: %s, got %s", "nil", error.Error())
		t.Fail()
	}
	body, _ = ioutil.ReadAll(hreq.Body)
	request = requests.CreateFunctionRequest{}
	error = json.Unmarshal(body, &request)
	if error != nil {
		t.Errorf("SetRealtimeParams - unable to unmarshal the updated request")
		t.Fail()
	}
	if request.Labels == nil {
		t.Errorf("SetRealtimeParams - empty Labels")
		t.Fail()
	}

	realtime := extractLabelRealValue((*request.Labels)["realtime"], float64(0))
	size := extractLabelRealValue((*request.Labels)["functionsize"], float64(1.0))
	duration := extractLabelValue((*request.Labels)["duration"], uint64(60))
	if realtime != 20 {
		t.Errorf("SetRealtimeParams - realtime want: %s, got %f", "20", realtime)
		t.Fail()
	}
	if size != 0.1 {
		t.Errorf("SetRealtimeParams - size want: %s, got %f", "0.1", size)
		t.Fail()
	}
	if duration != 120 {
		t.Errorf("SetRealtimeParams - duration want: %s, got %d", "120", duration)
		t.Fail()
	}
}

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
	functionSize := float64(1)
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
		FunctionSize:      functionSize,
		Duration:          functionDuration,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}

	realtime, size, duration, replicas, err := rm.DeploymentRealtimeParams("noinfo")
	if err == nil {
		t.Errorf("DeploymentRealtimeParams - get noinfo, want: %s, got %s", "function not found error", "nil")
		t.Fail()
	}

	realtime, size, duration, replicas, err = rm.DeploymentRealtimeParams("test")
	if err != nil {
		t.Errorf("DeploymentRealtimeParams - get test error, want: %s, got %s", "nil", err.Error())
		t.Fail()
	}
	if realtime != realTime {
		t.Errorf("DeploymentRealtimeParams - get test realtime, want: %f, got %f", realTime, realtime)
		t.Fail()
	}
	if functionSize != size {
		t.Errorf("DeploymentRealtimeParams - get test size, want: %f, got %f", functionSize, size)
		t.Fail()
	}
	if duration != functionDuration {
		t.Errorf("DeploymentRealtimeParams - get test duration, want: %d, got %d", functionDuration, duration)
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
	functionSize := float64(1)
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
		FunctionSize:      functionSize,
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
	functionSize := float64(1)
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
		FunctionSize:      functionSize,
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
	functionSize := float64(1)
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
		FunctionSize:      functionSize,
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
