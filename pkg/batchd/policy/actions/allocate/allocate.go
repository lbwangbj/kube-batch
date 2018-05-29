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

package allocate

import (
	"github.com/golang/glog"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy/framework"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy/util"
)

type allocateAction struct {
}

func New() *allocateAction {
	return &allocateAction{}
}

func (alloc *allocateAction) Name() string {
	return "allocate"
}

func (alloc *allocateAction) Initialize() {}

func (alloc *allocateAction) Execute(ssn *framework.Session) {
	glog.V(4).Infof("Enter Allocate ...")
	defer glog.V(4).Infof("Leaving Allocate ...")

	jobs := util.NewPriorityQueue(ssn.OrderJobs)
	for _, q := range ssn.Queues {
		for _, job := range q.Jobs {
			jobs.Push(job)
		}
	}

	for {
		if jobs.Empty() {
			break
		}
		job := jobs.Pop().(*api.JobInfo)

		for _, task := range job.Tasks[api.Pending] {
			assigned := false
			for _, node := range ssn.Nodes {
				if task.Resreq.Less(node.Idle) {
					if err := ssn.Allocate(task.UID, node.UID); err != nil {
						glog.Errorf("Failed to allocate %v to Task %v on %v in Session %v",
							task.Resreq, task.UID, node.Name, ssn.ID)
						continue
					}
					assigned = true
					break
				}
			}

			if assigned {
				jobs.Push(job)
			}

			// Handle one pending task in each loop.
			break
		}
	}
}

func (alloc *allocateAction) UnInitialize() {}
