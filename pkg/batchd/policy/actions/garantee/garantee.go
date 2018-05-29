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

package garantee

import (
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy/framework"
)

type garanteeAction struct {
}

func New() *garanteeAction {
	return &garanteeAction{}
}

// The unique name of allocator.
func (act *garanteeAction) Name() string {
	return "garantee"
}

// Initialize initializes the allocator plugins.
func (act *garanteeAction) Initialize() {}

// Execute allocates the cluster's resources into each queue.
func (act *garanteeAction) Execute(ssn *framework.Session) {
	for _, job := range ssn.Jobs {
		occupied := 0
		for status, tasks := range job.Tasks {
			if api.OccupiedResources(status) {
				occupied += len(tasks)
			}
		}

		alloc := map[api.TaskID]api.NodeID{}
		for _, task := range job.Tasks[api.Pending] {
			if occupied >= job.MinAvailable {
				break
			}

			for _, node := range ssn.Nodes {
				if task.Resreq.Less(node.Idle) {
					if err := ssn.Allocate(task.UID, node.UID); err == nil {
						occupied++
						alloc[task.UID] = node.UID
					}
				}
			}
		}

		if occupied < job.MinAvailable {
			for taskID, nodeID := range alloc {
				ssn.Deallocate(taskID, nodeID)
			}
		}
	}
}

// UnIntialize un-initializes the allocator plugins.
func (act *garanteeAction) UnInitialize() {

}
