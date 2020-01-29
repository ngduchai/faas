// Copyright (c) OpenFaaS Author(s). All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package scaling

import (
	"log"
	"sync"
	"time"
)

// FunctionMeta holds the last refresh and any other
// meta-data needed for caching.
type FunctionMeta struct {
	LastRefresh          time.Time
	ServiceQueryResponse ServiceQueryResponse
	/*
		Because invocation requests are requested by remote clients, fast rate requests
		may experience jitter thus, we need the smoothness parameters to overcome the
		problem. The solution is follows: The smoothness could be varied between
			    -SmoothnessBound <= Smoothness <= +SmoothnessBound
		Everytime a request appear, we calculate the interval between its arrival and
		the most recent request, if
		    - the interval >= invocation threshold:
			accept the request
			then add the exceeded quantity to the smoothless
		    - the interval < invocation threshold:
			subtract the gap of [invocation threshold - inteval] from the
			smoothness. If the smoothness goes below the lowerbound, skip the
			request. Otherwise, accept
	*/

	Smoothness float64
}

// Expired find out whether the cache item has expired with
// the given expiry duration from when it was stored.
func (fm *FunctionMeta) Expired(expiry time.Duration) bool {
	return time.Now().After(fm.LastRefresh.Add(expiry))
}

// FunctionCache provides a cache of Function replica counts
type FunctionCache struct {
	Cache  map[string]*FunctionMeta
	Expiry time.Duration
	Sync   sync.RWMutex
}

// Set replica count for functionName
func (fc *FunctionCache) Set(functionName string, serviceQueryResponse ServiceQueryResponse) {
	fc.Sync.Lock()
	defer fc.Sync.Unlock()

	if _, exists := fc.Cache[functionName]; !exists {
		fc.Cache[functionName] = &FunctionMeta{Smoothness: 0.0}
	} else {
		//serviceQueryResponse.PastAllocations = fc.Cache[functionName].ServiceQueryResponse.PastAllocations
		serviceQueryResponse.PastAllocation = fc.Cache[functionName].ServiceQueryResponse.PastAllocation
	}

	fc.Cache[functionName].LastRefresh = time.Now()
	fc.Cache[functionName].ServiceQueryResponse = serviceQueryResponse
	// entry.LastRefresh = time.Now()
	// entry.ServiceQueryResponse = serviceQueryResponse
}

// Get replica count for functionName
func (fc *FunctionCache) Get(functionName string) (ServiceQueryResponse, bool) {
	replicas := ServiceQueryResponse{
		AvailableReplicas: 0,
	}

	hit := false
	fc.Sync.RLock()
	defer fc.Sync.RUnlock()

	if val, exists := fc.Cache[functionName]; exists {
		replicas = val.ServiceQueryResponse
		hit = !val.Expired(fc.Expiry)
	}

	return replicas, hit
}

// Update Allcation list
func (fc *FunctionCache) UpdateInvocation(functionName string, invokeTime time.Time) (uint64, time.Duration, bool) {
	totalInvocation := uint64(0)
	gapLength := time.Since(invokeTime)
	//hit := false
	added := false
	//fc.Sync.RLock()
	//defer fc.Sync.RUnlock()
	fc.Sync.Lock()
	defer fc.Sync.Unlock()

	//log.Printf("Check function realtime for %s", functionName)

	/* Because request can experience jitter so we need to add */

	if val, exists := fc.Cache[functionName]; exists {
		//hit = !val.Expired(fc.Expiry)
		/*
			allocations := &val.ServiceQueryResponse.PastAllocations
			for allocations.Len() > 0 {
				pastInvocationTime := allocations.Front()
				diff := invokeTime.Sub(pastInvocationTime.Value.(time.Time))
				log.Printf("%f %f", diff.Seconds(), 1.0/val.ServiceQueryResponse.Realtime)
				//if diff.Seconds() > 1.0 {
				if diff.Seconds() > (1.0 / val.ServiceQueryResponse.Realtime) {
					allocations.Remove(pastInvocationTime)
					if allocations.Len() == 0 {
					}
				} else {
					break
				}
			}
			totalInvocation = uint64(allocations.Len())
			//if val.ServiceQueryResponse.Realtime > float64(totalInvocation) {
			if 1.0 > float64(totalInvocation) {
				allocations.PushBack(invokeTime)
				added = true
			}
			gapLength = invokeTime.Sub(allocations.Front().Value.(time.Time))
		*/
		intervalBound := 1.0 / val.ServiceQueryResponse.Realtime
		gapLength := invokeTime.Sub(val.ServiceQueryResponse.PastAllocation)
		diff := gapLength.Seconds() - intervalBound
		if diff >= 0 || val.Smoothness+diff > -1.5*intervalBound {
			added = true
			val.ServiceQueryResponse.PastAllocation = invokeTime
			val.Smoothness += diff
			if val.Smoothness > 1.5*intervalBound {
				val.Smoothness = 1.5 * intervalBound
			}
		}
		log.Printf("%f %f %f", diff, intervalBound, val.Smoothness)
	}

	//return totalInvocation, gapLength, hit, added
	return totalInvocation, gapLength, added
}
