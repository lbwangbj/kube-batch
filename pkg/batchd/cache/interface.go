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

package cache

import (
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
)

// Cache collects pods/nodes/queues information
// and provides information snapshot
type Cache interface {
	// Run start informer
	Run(stopCh <-chan struct{})

	// Snapshot deep copy overall cache information into snapshot
	Snapshot() *CacheSnapshot

	// WaitForCacheSync waits for all cache synced
	WaitForCacheSync(stopCh <-chan struct{}) bool

	// UpdateTaskStatus updates task's status to the target status.
	// UpdateTaskStatus(taskID types.UID, status api.TaskStatus) error

	// Bind binds a task to the target host.
	Bind(taskID api.TaskID, host api.NodeID) error

	// Evict evicts a task from host.
	Evict(taskID api.TaskID) error
}
