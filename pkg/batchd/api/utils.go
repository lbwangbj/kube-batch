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
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcache "k8s.io/client-go/tools/cache"

	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/apis/v1"
)

type LessFn func(interface{}, interface{}) bool
type CompareFn func(interface{}, interface{}) int

// PodKey returns the string key of a pod.
func PodKey(pod *v1.Pod) string {
	if key, err := clientcache.MetaNamespaceKeyFunc(pod); err != nil {
		return fmt.Sprintf("%v/%v", pod.Namespace, pod.Name)
	} else {
		return key
	}
}

func GetTaskID(pod *v1.Pod) TaskID {
	return TaskID(pod.UID)
}

func GetQueueID(queue *arbv1.Queue) QueueID {
	return QueueID(queue.Namespace)
}

func GetNodeID(node *v1.Node) NodeID {
	return NodeID(node.Name)
}

func GetPodOwner(pod *v1.Pod) JobID {
	metapod, err := meta.Accessor(pod)
	if err != nil {
		return ""
	}

	controllerRef := metav1.GetControllerOf(metapod)
	if controllerRef != nil {
		return JobID(controllerRef.UID)
	}

	return ""
}

func GetPodStatus(pod *v1.Pod) TaskStatus {

	if pod.DeletionTimestamp != nil {
		return Releasing
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		if len(pod.Spec.NodeName) == 0 {
			return Pending
		}
		return Bound
	case v1.PodRunning:
		return Running
	case v1.PodFailed:
		return Failed
	case v1.PodSucceeded:
		return Succeeded
	default:
		return Unknown
	}
}

func OccupiedResources(status TaskStatus) bool {
	switch status {
	case Running, Binding, Bound, Allocated:
		return true
	}
	return false
}
