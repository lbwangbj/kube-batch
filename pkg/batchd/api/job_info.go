/*
Copyright 2017 The Kubernetes Authors.

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

package api

import (
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

type TaskID types.UID
type JobID types.UID
type QueueID string

type TaskInfo struct {
	UID   TaskID
	Job   JobID
	Queue QueueID

	Node     NodeID
	Status   TaskStatus
	Priority int32

	Resreq *Resource

	Pod *v1.Pod
}

type taskMap map[TaskID]*TaskInfo
type taskStatusMap map[TaskStatus]taskMap

func NewTaskInfo(pod *v1.Pod) *TaskInfo {
	req := EmptyResource()

	for _, c := range pod.Spec.Containers {
		req.Add(NewResource(c.Resources.Requests))
	}

	pi := &TaskInfo{
		UID:      TaskID(pod.UID),
		Job:      JobID(GetPodOwner(pod)),
		Queue:    QueueID(pod.Namespace),
		Node:     NodeID(pod.Spec.NodeName),
		Priority: 1,
		Pod:      pod,
		Resreq:   req,
	}

	if pod.Spec.Priority != nil {
		pi.Priority = *pod.Spec.Priority
	}

	pi.Status = GetPodStatus(pod)

	return pi
}

func (pi *TaskInfo) Clone() *TaskInfo {
	return &TaskInfo{
		UID:      pi.UID,
		Job:      pi.Job,
		Node:     pi.Node,
		Status:   pi.Status,
		Priority: pi.Priority,
		Pod:      pi.Pod,
		Resreq:   pi.Resreq.Clone(),
	}
}

type JobInfo struct {
	UID   JobID
	Queue QueueID

	PdbName      string
	MinAvailable int

	Allocated    *Resource
	TotalRequest *Resource

	Tasks taskStatusMap

	Labels       labels.Set
	NodeSelector map[string]string
}

func NewJobInfo(uid JobID) *JobInfo {
	return &JobInfo{
		UID:          uid,
		PdbName:      "",
		MinAvailable: 0,
		Allocated:    EmptyResource(),
		TotalRequest: EmptyResource(),
		Tasks:        taskStatusMap{},

		NodeSelector: make(map[string]string),
	}
}

func (ps *JobInfo) PopTask(status TaskStatus) *TaskInfo {
	return nil
}

func (ps *JobInfo) PushTask(t *TaskInfo) {
	ps.Tasks[t.Status][t.UID] = t
}

func (ps *JobInfo) AddTaskInfo(pi *TaskInfo) {
	if _, found := ps.Tasks[pi.Status]; !found {
		ps.Tasks[pi.Status] = make(taskMap)
	}
	ps.Tasks[pi.Status][pi.UID] = pi

	if OccupiedResources(pi.Status) {
		ps.Allocated.Add(pi.Resreq)
	}

	ps.TotalRequest.Add(pi.Resreq)

	// Update PodSet Labels
	// assume all pods in the same PodSet have same labels
	if len(ps.Labels) == 0 && len(pi.Pod.Labels) != 0 {
		ps.Labels = pi.Pod.Labels
	}

	// Update PodSet NodeSelector
	// assume all pods in the same PodSet have same NodeSelector
	if len(ps.NodeSelector) == 0 && len(pi.Pod.Spec.NodeSelector) != 0 {
		for k, v := range pi.Pod.Spec.NodeSelector {
			ps.NodeSelector[k] = v
		}
	}
}

func (ps *JobInfo) DeleteTaskInfo(pi *TaskInfo) {
	tasks, found := ps.Tasks[pi.Status]
	if !found {
		return
	}
	task, found := tasks[pi.UID]
	if !found {
		return
	}

	if OccupiedResources(task.Status) {
		ps.Allocated.Sub(task.Resreq)
	}
	ps.TotalRequest.Sub(task.Resreq)

	delete(tasks, pi.UID)
}

func (ps *JobInfo) Clone() *JobInfo {
	info := &JobInfo{
		UID:          ps.UID,
		PdbName:      ps.PdbName,
		MinAvailable: ps.MinAvailable,
		Allocated:    ps.Allocated.Clone(),
		TotalRequest: ps.TotalRequest.Clone(),

		NodeSelector: ps.NodeSelector,
	}

	for status, tasks := range ps.Tasks {
		info.Tasks[status] = make(taskMap)
		for _, task := range tasks {
			info.Tasks[status][task.UID] = task.Clone()
		}
	}

	return info
}
