// Copyright (c) OpenFaaS Author(s). All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package scaling

import (
	//"container/list"
	"time"
)

// ServiceQuery provides interface for replica querying/setting
type ServiceQuery interface {
	GetReplicas(service string) (response ServiceQueryResponse, err error)
	SetReplicas(service string, count uint64) error
}

// ServiceQueryResponse response from querying a function status
type ServiceQueryResponse struct {
	Replicas          uint64
	MaxReplicas       uint64
	MinReplicas       uint64
	ScalingFactor     uint64
	AvailableReplicas uint64
	Realtime          float64
	CPU               int64
	Memory            int64
	Duration          uint64
	//PastAllocations   list.List
	PastAllocation time.Time
}
