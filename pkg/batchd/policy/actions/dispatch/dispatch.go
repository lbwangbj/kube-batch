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

package dispatch

import (
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy/framework"
)

type dispatchAction struct {
}

func New() *dispatchAction {
	return &dispatchAction{}
}

// The unique name of allocator.
func (act *dispatchAction) Name() string {
	return "dispatch"
}

// Initialize initializes the allocator plugins.
func (act *dispatchAction) Initialize() {}

// Execute allocates the cluster's resources into each queue.
func (act *dispatchAction) Execute(ssn *framework.Session) {
	for _, q := range ssn.Queues {
		for _, j := range q.Jobs {
			for _, task := range j.Tasks[api.Allocated] {
				ssn.Bind(task.UID, task.Node)
			}

			for _, task := range j.Tasks[api.Evicted] {
				ssn.Evict(task.UID)
			}
		}
	}
}

// UnIntialize un-initializes the allocator plugins.
func (act *dispatchAction) UnInitialize() {

}
