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
	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/apis/v1"
)

type QueueInfo struct {
	UID QueueID

	// All jobs belong to this Queue
	Jobs map[JobID]*JobInfo

	Queue *arbv1.Queue
}

func NewQueueInfo(queue *arbv1.Queue) *QueueInfo {
	if queue == nil {
		return &QueueInfo{
			Queue: nil,
		}
	}

	return &QueueInfo{
		UID: QueueID(queue.Namespace),

		Queue: queue,
	}
}

func (ci *QueueInfo) SetQueue(queue *arbv1.Queue) {
	if queue == nil {
		ci.Queue = queue
		return
	}

	ci.Queue = queue
}

func (ci *QueueInfo) AddPod(pi *TaskInfo) {
	if _, found := ci.Jobs[pi.Job]; !found {
		ci.Jobs[pi.Job] = NewJobInfo(pi.Job)
	}
	ci.Jobs[pi.Job].AddTaskInfo(pi)
}

func (ci *QueueInfo) RemovePod(pi *TaskInfo) {
	if _, found := ci.Jobs[pi.Job]; found {
		ci.Jobs[pi.Job].DeleteTaskInfo(pi)
	}
}

func (ci *QueueInfo) AddPdb(pi *PdbInfo) {
	for _, ps := range ci.Jobs {
		if len(ps.PdbName) != 0 {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pi.Pdb.Spec.Selector)
		if err != nil {
			glog.V(4).Infof("LabelSelectorAsSelector fail for pdb %s", pi.Name)
			continue
		}
		// One PDB is fully for one PodSet
		// TODO(jinzhej): handle PDB cross different PodSet later on demand
		if selector.Matches(labels.Set(ps.Labels)) {
			ps.PdbName = pi.Name
			if pi.Pdb.Spec.MinAvailable.Type == intstr.Int {
				// support integer MinAvailable in PodDisruptionBuget
				// TODO(jinzhej): percentage MinAvailable, integer/percentage MaxUnavailable will be supported on demand
				ps.MinAvailable = int(pi.Pdb.Spec.MinAvailable.IntVal)
			}
		}
	}
}

func (ci *QueueInfo) RemovePdb(pi *PdbInfo) {
	for _, ps := range ci.Jobs {
		if len(ps.PdbName) == 0 {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pi.Pdb.Spec.Selector)
		if err != nil {
			glog.V(4).Infof("LabelSelectorAsSelector fail for pdb %s", pi.Name)
			continue
		}
		if selector.Matches(labels.Set(ps.Labels)) {
			ps.PdbName = ""
			ps.MinAvailable = 0
		}
	}
}

func (ci *QueueInfo) Clone() *QueueInfo {
	info := &QueueInfo{
		Queue: ci.Queue,

		Jobs: make(map[JobID]*JobInfo),
	}

	for id, ps := range ci.Jobs {
		info.Jobs[id] = ps.Clone()
	}

	return info
}
