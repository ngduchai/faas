package realtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"testing"

	"github.com/ngduchai/faas/gateway/plugin"
	"github.com/ngduchai/faas/gateway/requests"
	"github.com/ngduchai/faas/gateway/scaling"
)

func Test_ParseCreateFunctionRequest_with_realtime(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	request.Realtime = 10
	request.Limits = &requests.FunctionResources{
		CPU:    "1",
		Memory: "10024MiB",
	}
	request.Resources = &requests.FunctionResources{
		CPU:    "0.01",
		Memory: "32MiB",
	}
	request.Timeout = 100
	body, _ := json.Marshal(request)
	reader := bytes.NewReader(body)
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=1", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	cfreq, error := rm.ParseCreateFunctionRequest(hreq)

	if cfreq.Service != "test" {
		t.Errorf("ParseCreateFunctionRequest - functionName want: %s, got %s", "test", cfreq.Service)
		t.Fail()
	}

	if cfreq.Realtime != 10.0 {
		t.Errorf("ParseCreateFunctionRequest - Realtime want: %s, got %f", "10.0", cfreq.Realtime)
		t.Fail()
	}
	if cfreq.Timeout != 100 {
		t.Errorf("ParseCreateFunctionRequest - Timeout: want: %s, got %d", "100", cfreq.Timeout)
		t.Fail()
	}
	if error != nil {
		t.Errorf("ParseCreateFunctionRequest - error want: %s, got %s", "nil", error.Error())
		t.Fail()
	}

}

func Test_ParseCreateFunctionRequest_incorrect_message(t *testing.T) {
	reader := bytes.NewReader([]byte("Hello world"))
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=3", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	//functionName, realtime, size, duration, error := rm.RequestRealtimeParams(hreq)
	_, err := rm.ParseCreateFunctionRequest(hreq)

	if err == nil {
		t.Errorf("ParseCreateFunctionRequest - error want: %s, got %s", " != nil", "nil")
		t.Fail()
	}

}

func Test_DumpCreateFunctionRequest_correct_params(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	request.Realtime = 10
	request.Limits = &requests.FunctionResources{
		CPU:    "1",
		Memory: "10024MiB",
	}
	request.Resources = &requests.FunctionResources{
		CPU:    "0.01",
		Memory: "32MiB",
	}
	request.Timeout = 100
	reader := bytes.NewReader([]byte("Hello world"))
	hreq, _ := http.NewRequest(http.MethodPost, "/?test=3", reader)
	hreq.Header.Set("X-Source", "unit-test")

	rm := ResourceManager{}
	err := rm.DumpCreateFunctionRequest(hreq, &request)

	if err != nil {
		t.Errorf("DumpCreateFunctionRequest - error -- Unable to dump request: %s", err.Error())
		t.Fail()
	}

	req, err := rm.ParseCreateFunctionRequest(hreq)
	if err != nil {
		t.Errorf("DumpCreateFunctionRequest - error -- Dumped request is unreadable: %s", err.Error())
		t.Fail()
	}

	if req.Realtime != request.Realtime {
		t.Errorf("DumpCreateFunctionRequest - Realtime: want: %f, got %f", request.Realtime, req.Realtime)
		t.Fail()
	}
	if req.Timeout != request.Timeout {
		t.Errorf("DumpCreateFunctionRequest - Timeout: want: %d, got %d", request.Timeout, req.Timeout)
		t.Fail()
	}
	if req.Service != request.Service {
		t.Errorf("DumpCreateFunctionRequest - functionName: want: %s, got %s", request.Service, req.Service)
		t.Fail()
	}
	if req.Resources.CPU != request.Resources.CPU {
		t.Errorf("DumpCreateFunctionRequest - CPU: want: %s, got %s", request.Resources.CPU, req.Resources.CPU)
		t.Fail()
	}
	if req.Resources.Memory != request.Resources.Memory {
		t.Errorf("DumpCreateFunctionRequest - CPU: want: %s, got %s", request.Resources.Memory, req.Resources.Memory)
		t.Fail()
	}
}

func Test_ContainerConcurrency(t *testing.T) {
	request := requests.CreateFunctionRequest{}
	request.Service = "test"
	request.Labels = &map[string]string{}
	request.Realtime = 10
	request.Limits = &requests.FunctionResources{
		CPU:    "1",
		Memory: "10240Mi",
	}
	request.Resources = &requests.FunctionResources{
		CPU:    "0.01",
		Memory: "32Mi",
	}
	request.Timeout = 100
	expectedConcurrency := int(math.Min(1.0/0.01, 10240.0/32.0))
	rm := ResourceManager{}
	concurrency, err := rm.ContainerConcurrency(&request)
	if err != nil {
		t.Errorf("ContainerConcurrency - error -- Cannot get concurrency: %s", err.Error())
		t.Fail()
	}
	if concurrency != expectedConcurrency {
		t.Errorf("ContainerConcurrency: want: %d, got %d", expectedConcurrency, concurrency)
		t.Fail()
	}
	request.Resources.Memory = "1024Mi"
	expectedConcurrency = int(math.Min(1.0/0.01, 10240.0/1024.0))
	concurrency, err = rm.ContainerConcurrency(&request)
	if err != nil {
		t.Errorf("ContainerConcurrency - error -- Cannot get concurrency: %s", err.Error())
		t.Fail()
	}
	if concurrency != expectedConcurrency {
		t.Errorf("ContainerConcurrency: want: %d, got %d", expectedConcurrency, concurrency)
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
	}
	return scaling.ServiceQueryResponse{}, errors.New("function not found")
}

func (sq TestServiceQuery) SetReplicas(serviceName string, count uint64) error {
	if _, ok := sq.Info[serviceName]; ok {
		sq.Info[serviceName].Replicas = count
		return nil
	}
	return errors.New("Function not found")
}

func Test_DeploymentRealtimeParams(t *testing.T) {
	f := scaling.GetScalerInstance()
	replicas := uint64(10)
	maxReplicas := uint64(10)
	minReplicas := uint64(0)
	scalingFactor := uint64(2)
	availableReplicas := uint64(2)
	functionRealtime := float64(1)
	functionConcurrency := uint64(10)
	functionTimeout := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          functionRealtime,
		Concurrency:       functionConcurrency,
		Timeout:           functionTimeout,
	}
	f.Config.ServiceQuery = serviceQuery

	rm := ResourceManager{}

	realtime, concurrency, timeout, replicas, err := rm.GetRealtimeParams("noinfo")
	if err == nil {
		t.Errorf("DeploymentRealtimeParams - get noinfo, want: %s, got %s", "function not found error", "nil")
		t.Fail()
	}

	realtime, concurrency, timeout, replicas, err = rm.GetRealtimeParams("test")
	if err != nil {
		t.Errorf("DeploymentRealtimeParams - get test error, want: %s, got %s", "nil", err.Error())
		t.Fail()
	}
	if realtime != functionRealtime {
		t.Errorf("DeploymentRealtimeParams - get test realtime, want: %f, got %f", functionRealtime, realtime)
		t.Fail()
	}
	if functionConcurrency != concurrency {
		t.Errorf("DeploymentRealtimeParams - get test size, want: %d, got %d", functionConcurrency, concurrency)
		t.Fail()
	}
	if timeout != functionTimeout {
		t.Errorf("DeploymentRealtimeParams - get test duration, want: %d, got %d", functionTimeout, timeout)
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
	functionRealtime := float64(1)
	functionConcurrency := uint64(1)
	functionTimeout := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          functionRealtime,
		Concurrency:       functionConcurrency,
		Timeout:           functionTimeout,
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
	functionRealtime := float64(1)
	functionConcurrency := uint64(1)
	functionTimeout := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          functionRealtime,
		Concurrency:       functionConcurrency,
		Timeout:           functionTimeout,
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
	functionRealtime := float64(1)
	functionConcurrency := uint64(1)
	functionTimeout := uint64(10)
	serviceQuery := TestServiceQuery{}
	serviceQuery.Info = map[string]*scaling.ServiceQueryResponse{}
	serviceQuery.Info["test"] = &scaling.ServiceQueryResponse{
		Replicas:          replicas,
		MaxReplicas:       maxReplicas,
		MinReplicas:       minReplicas,
		ScalingFactor:     scalingFactor,
		AvailableReplicas: availableReplicas,
		Realtime:          functionRealtime,
		Concurrency:       functionConcurrency,
		Timeout:           functionTimeout,
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
