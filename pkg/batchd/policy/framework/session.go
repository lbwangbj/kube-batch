/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"sync"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/cache"
)

type Session struct {
	ID types.UID

	cache         cache.Cache
	plugins       map[string]Plugin
	eventHandlers []*EventHandler
	jobOrderFns   []api.CompareFn
	mutex         sync.Mutex

	Tasks  map[api.TaskID]*api.TaskInfo
	Jobs   map[api.JobID]*api.JobInfo
	Queues map[api.QueueID]*api.QueueInfo
	Nodes  map[api.NodeID]*api.NodeInfo
}

func (ssn *Session) Bind(taskID api.TaskID, host api.NodeID) {
	if err := ssn.cache.Bind(taskID, host); err != nil {
		glog.Errorf("Failed to bind Task %v to host %v in Session %v: %v",
			taskID, host, ssn.ID, err)
	}
}

func (ssn *Session) Evict(taskID api.TaskID) {
	if err := ssn.cache.Evict(taskID); err != nil {
		glog.Errorf("Failed to eveict Task %v in Session %v: %v",
			taskID, ssn.ID, err)
	}
}

func (ssn *Session) Allocate(taskID api.TaskID, host api.NodeID) error {
	task := ssn.Tasks[taskID]
	job := ssn.Jobs[task.Job]
	node := ssn.Nodes[host]

	// Delete Pending task
	job.DeleteTaskInfo(task)

	// Added it back as allocated tasks
	task.Status = api.Allocated
	job.AddTaskInfo(task)

	// Update node Idle resource
	node.Idle.Sub(task.Resreq)

	for _, eh := range ssn.eventHandlers {
		if eh.OnAllocated == nil {
			continue
		}
		eh.OnAllocated(&Event{
			Task: task,
			Job:  job,
			Node: node,
		})
	}

	return nil
}

func (ssn *Session) Deallocate(taskID api.TaskID, host api.NodeID) error {
	task := ssn.Tasks[taskID]
	job := ssn.Jobs[task.Job]
	node := ssn.Nodes[host]

	// Delete Pending task
	job.DeleteTaskInfo(task)

	// Added it back as pending task
	task.Status = api.Pending
	job.AddTaskInfo(task)

	// Update node Idle resource
	node.Idle.Add(task.Resreq)

	for _, eh := range ssn.eventHandlers {
		if eh.OnDeallocated == nil {
			continue
		}
		eh.OnDeallocated(&Event{
			Task: task,
			Job:  job,
			Node: node,
		})
	}

	return nil
}

func (ssn *Session) OrderJobs(l interface{}, r interface{}) bool {
	for _, jo := range ssn.jobOrderFns {
		if jo(l, r) == 0 {
			continue
		}

		if jo(l, r) < 0 {
			return true
		}
	}
	return false
}

func (ssn *Session) RegisterJobOrderFn(j api.CompareFn) {
	ssn.jobOrderFns = append(ssn.jobOrderFns, j)
}

func (ssn *Session) AddEventHandler(eh *EventHandler) {
	ssn.eventHandlers = append(ssn.eventHandlers, eh)
}
