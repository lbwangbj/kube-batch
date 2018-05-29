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
	"fmt"
	"strings"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	"k8s.io/client-go/informers"
	clientv1 "k8s.io/client-go/informers/core/v1"
	policyv1 "k8s.io/client-go/informers/policy/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/apis/v1"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/client"
	informerfactory "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/client/informers"
	arbclient "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/client/informers/v1"
)

func newSchedulerCache(config *rest.Config, schedulerName string) *SchedulerCache {
	sc := &SchedulerCache{
		Nodes:  make(map[api.NodeID]*api.NodeInfo),
		Queues: make(map[api.QueueID]*api.QueueInfo),
		Jobs:   make(map[api.JobID]*api.JobInfo),
		Tasks:  make(map[api.TaskID]*api.TaskInfo),

		Pdbs: make(map[string]*api.PdbInfo),
	}

	sc.kubeclient = kubernetes.NewForConfigOrDie(config)
	informerFactory := informers.NewSharedInformerFactory(sc.kubeclient, 0)

	// create informer for node information
	sc.nodeInformer = informerFactory.Core().V1().Nodes()
	sc.nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sc.AddNode,
			UpdateFunc: sc.UpdateNode,
			DeleteFunc: sc.DeleteNode,
		},
		0,
	)

	// create informer for pod information
	sc.podInformer = informerFactory.Core().V1().Pods()
	sc.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.Pod:
					pod := obj.(*v1.Pod)
					if strings.Compare(pod.Spec.SchedulerName, schedulerName) == 0 && pod.Status.Phase == v1.PodPending {
						return true
					}
					return pod.Status.Phase == v1.PodRunning
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddPod,
				UpdateFunc: sc.UpdatePod,
				DeleteFunc: sc.DeletePod,
			},
		})

	// create queue informer
	queueClient, _, err := client.NewClient(config)
	if err != nil {
		panic(err)
	}

	queueInformerFactory := informerfactory.NewSharedInformerFactory(queueClient, 0)
	// create informer for Queue information
	sc.queueInformer = queueInformerFactory.Queue().Queues()
	sc.queueInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *arbv1.Queue:
					glog.V(4).Infof("Filter Queue name(%s) namespace(%s)\n", t.Name, t.Namespace)
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddQueue,
				UpdateFunc: sc.UpdateQueue,
				DeleteFunc: sc.DeleteQueue,
			},
		})

	// create informer for pdb information
	sc.pdbInformer = informerFactory.Policy().V1beta1().PodDisruptionBudgets()
	sc.pdbInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1beta1.PodDisruptionBudget:
					glog.V(4).Infof("Filter pdb name(%s)\n", t.Name)
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddPDB,
				DeleteFunc: sc.DeletePDB,
			},
		})

	return sc
}

func (sc *SchedulerCache) Run(stopCh <-chan struct{}) {
	go sc.podInformer.Informer().Run(stopCh)
	go sc.nodeInformer.Informer().Run(stopCh)
	go sc.queueInformer.Informer().Run(stopCh)
	go sc.pdbInformer.Informer().Run(stopCh)
}

func (sc *SchedulerCache) WaitForCacheSync(stopCh <-chan struct{}) bool {
	return cache.WaitForCacheSync(stopCh,
		sc.podInformer.Informer().HasSynced,
		sc.nodeInformer.Informer().HasSynced,
		sc.queueInformer.Informer().HasSynced)
}

// nonTerminatedPod selects pods that are non-terminal (pending and running).
func nonTerminatedPod(pod *v1.Pod) bool {
	if pod.Status.Phase == v1.PodSucceeded ||
		pod.Status.Phase == v1.PodFailed ||
		pod.Status.Phase == v1.PodUnknown {
		return false
	}
	return true
}

// func (sc *SchedulerCache) UpdateTaskStatus(taskID types.UID, status api.TaskStatus) error {
// 	task, found := sc.Tasks[taskID]
// 	if !found {
// 		return fmt.Errorf("failed to find Task %v in cache when updating status", taskID)
// 	}

// 	// TODO(k82cn): Update resource usage of node.

// 	// TODO(k82cn): Check whehter status trasfer is right.
// 	task.Status = status
// 	return nil
// }

// Assumes that lock is already acquired.
func (sc *SchedulerCache) addPod(pod *v1.Pod) error {
	if _, ok := sc.Tasks[api.TaskID(pod.UID)]; ok {
		return fmt.Errorf("pod %v exist", pod.UID)
	}

	// for assumed pod, check coming pod status
	// running, remove from assumed pods and add pod to cache
	// pending with host(same or different), remove from assumed pods and add pod to cache
	// pending without host, do nothing for the pod due to it is assumed by policy
	// if _, ok := sc.assumedPodStates[key]; ok {
	// 	if pod.Status.Phase == v1.PodPending && len(pod.Spec.NodeName) == 0 {
	// 		return fmt.Errorf("pending pod %s without hostname is assumed", pod.Name)
	// 	} else {
	// 		delete(sc.assumedPodStates, key)
	// 	}
	// }

	pi := api.NewTaskInfo(pod)

	// Add the pod to the host.
	nodeID := api.NodeID(pod.Spec.NodeName)
	if len(nodeID) > 0 {
		if sc.Nodes[nodeID] == nil {
			sc.Nodes[nodeID] = api.NewNodeInfo(nil)
		}
		sc.Nodes[nodeID].AddPod(pi)
	}

	if sc.Queues[pi.Queue] == nil {
		sc.Queues[pi.Queue] = api.NewQueueInfo(nil)
	}
	sc.Queues[pi.Queue].AddPod(pi)

	if sc.Jobs[pi.Job] == nil {
		sc.Jobs[pi.Job] = sc.Queues[pi.Queue].Jobs[pi.Job]
	}

	for _, pdb := range sc.Pdbs {
		sc.Queues[pi.Queue].AddPdb(pdb)
	}

	sc.Tasks[pi.UID] = pi

	return nil
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) updatePod(oldPod, newPod *v1.Pod) error {
	if err := sc.deletePod(oldPod); err != nil {
		return err
	}
	return sc.addPod(newPod)
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) deletePod(pod *v1.Pod) error {
	pi, ok := sc.Tasks[api.GetTaskID(pod)]
	if !ok {
		return fmt.Errorf("pod %v doesn't exist", pod)
	}

	delete(sc.Tasks, pi.UID)

	if len(pi.Node) != 0 {
		node := sc.Nodes[pi.Node]
		if node != nil {
			node.RemovePod(pi)
		}
	}

	queue := sc.Queues[pi.Queue]
	if queue != nil {
		queue.RemovePod(pi)
	}

	return nil
}

func (sc *SchedulerCache) AddPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Add pod(%s) into cache, status (%s)", pod.Name, pod.Status.Phase)
	err := sc.addPod(pod)
	if err != nil {
		glog.Errorf("Failed to add pod %s into cache: %v", pod.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) UpdatePod(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert oldObj to *v1.Pod: %v", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert newObj to *v1.Pod: %v", newObj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Update oldPod(%s) status(%s) newPod(%s) status(%s) in cache", oldPod.Name, oldPod.Status.Phase, newPod.Name, newPod.Status.Phase)
	err := sc.updatePod(oldPod, newPod)
	if err != nil {
		glog.Errorf("Failed to update pod %v in cache: %v", oldPod.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) DeletePod(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			glog.Errorf("Cannot convert to *v1.Pod: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("Cannot convert to *v1.Pod: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Delete pod(%s) status(%s) from cache", pod.Name, pod.Status.Phase)
	err := sc.deletePod(pod)
	if err != nil {
		glog.Errorf("Failed to delete pod %v from cache: %v", pod.Name, err)
		return
	}
	return
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) addNode(node *v1.Node) error {
	nodeID := api.GetNodeID(node)
	if sc.Nodes[nodeID] != nil {
		sc.Nodes[nodeID].SetNode(node)
	} else {
		sc.Nodes[nodeID] = api.NewNodeInfo(node)
	}

	return nil
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) updateNode(oldNode, newNode *v1.Node) error {
	// Did not delete the old node, just update related info, e.g. allocatable.
	nodeID := api.GetNodeID(newNode)
	if sc.Nodes[nodeID] != nil {
		sc.Nodes[nodeID].SetNode(newNode)
		return nil
	}

	return fmt.Errorf("node <%s> does not exist", newNode.Name)
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) deleteNode(node *v1.Node) error {
	nodeID := api.GetNodeID(node)
	if _, ok := sc.Nodes[nodeID]; !ok {
		return fmt.Errorf("node <%s> does not exist", node.Name)
	}
	delete(sc.Nodes, nodeID)
	return nil
}

func (sc *SchedulerCache) AddNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Node: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Add node(%s) into cache", node.Name)
	err := sc.addNode(node)
	if err != nil {
		glog.Errorf("Failed to add node %s into cache: %v", node.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) UpdateNode(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		glog.Errorf("Cannot convert oldObj to *v1.Node: %v", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		glog.Errorf("Cannot convert newObj to *v1.Node: %v", newObj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Update oldNode(%s) newNode(%s) in cache", oldNode.Name, newNode.Name)
	err := sc.updateNode(oldNode, newNode)
	if err != nil {
		glog.Errorf("Failed to update node %v in cache: %v", oldNode.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) DeleteNode(obj interface{}) {
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			glog.Errorf("Cannot convert to *v1.Node: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("Cannot convert to *v1.Node: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Delete node(%s) from cache", node.Name)
	err := sc.deleteNode(node)
	if err != nil {
		glog.Errorf("Failed to delete node %s from cache: %v", node.Name, err)
		return
	}
	return
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) addQueue(queue *arbv1.Queue) error {
	qi := api.NewQueueInfo(queue)

	if sc.Queues[qi.UID] != nil {
		sc.Queues[qi.UID].SetQueue(queue)
	} else {
		sc.Queues[qi.UID] = qi
	}

	return nil
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) updateQueue(oldQueue, newQueue *arbv1.Queue) error {
	qid := api.GetQueueID(newQueue)

	if sc.Queues[qid] != nil {
		sc.Queues[qid].SetQueue(newQueue)
		return nil
	}

	return fmt.Errorf("Queue <%s> does not exist", newQueue.Namespace)
}

// Assumes that lock is already acquired.
func (sc *SchedulerCache) deleteQueue(queue *arbv1.Queue) error {
	qid := api.GetQueueID(queue)

	if _, ok := sc.Queues[qid]; !ok {
		return fmt.Errorf("Queue %v doesn't exist", queue.Name)
	}
	delete(sc.Queues, qid)
	return nil
}

func (sc *SchedulerCache) AddQueue(obj interface{}) {
	queue, ok := obj.(*arbv1.Queue)
	if !ok {
		glog.Errorf("Cannot convert to *arbv1.Queue: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Add Queue(%s) into cache, spec(%#v)", queue.Name, queue.Spec)
	err := sc.addQueue(queue)
	if err != nil {
		glog.Errorf("Failed to add Queue %s into cache: %v", queue.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) UpdateQueue(oldObj, newObj interface{}) {
	oldQueue, ok := oldObj.(*arbv1.Queue)
	if !ok {
		glog.Errorf("Cannot convert oldObj to *arbv1.Queue: %v", oldObj)
		return
	}
	newQueue, ok := newObj.(*arbv1.Queue)
	if !ok {
		glog.Errorf("Cannot convert newObj to *arbv1.Queue: %v", newObj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Update oldQueue(%s) in cache, spec(%#v)", oldQueue.Name, oldQueue.Spec)
	glog.V(4).Infof("Update newQueue(%s) in cache, spec(%#v)", newQueue.Name, newQueue.Spec)
	err := sc.updateQueue(oldQueue, newQueue)
	if err != nil {
		glog.Errorf("Failed to update queue %s into cache: %v", oldQueue.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) DeleteQueue(obj interface{}) {
	var queue *arbv1.Queue
	switch t := obj.(type) {
	case *arbv1.Queue:
		queue = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		queue, ok = t.Obj.(*arbv1.Queue)
		if !ok {
			glog.Errorf("Cannot convert to *v1.Queue: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("Cannot convert to *v1.Queue: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.deleteQueue(queue)
	if err != nil {
		glog.Errorf("Failed to delete Queue %s from cache: %v", queue.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) addPDB(pdb *v1beta1.PodDisruptionBudget) error {
	pi := api.NewPdbInfo(pdb)
	sc.Pdbs[pi.Name] = pi
	for _, c := range sc.Queues {
		c.AddPdb(pi)
	}

	return nil
}

func (sc *SchedulerCache) deletePDB(pdb *v1beta1.PodDisruptionBudget) error {
	pi, exist := sc.Pdbs[pdb.Name]
	if !exist {
		return nil
	}
	delete(sc.Pdbs, pdb.Name)

	for _, c := range sc.Queues {
		c.RemovePdb(pi)
	}

	return nil
}

func (sc *SchedulerCache) AddPDB(obj interface{}) {
	pdb, ok := obj.(*v1beta1.PodDisruptionBudget)
	if !ok {
		glog.Errorf("Cannot convert to *v1beta1.PodDisruptionBudget: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	glog.V(4).Infof("Add PodDisruptionBudget(%s) into cache, spec(%#v)", pdb.Name, pdb.Spec)
	err := sc.addPDB(pdb)
	if err != nil {
		glog.Errorf("Failed to add PodDisruptionBudget %s into cache: %v", pdb.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) DeletePDB(obj interface{}) {
	var pdb *v1beta1.PodDisruptionBudget
	switch t := obj.(type) {
	case *v1beta1.PodDisruptionBudget:
		pdb = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pdb, ok = t.Obj.(*v1beta1.PodDisruptionBudget)
		if !ok {
			glog.Errorf("Cannot convert to *v1beta1.PodDisruptionBudget: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("Cannot convert to *v1beta1.PodDisruptionBudget: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.deletePDB(pdb)
	if err != nil {
		glog.Errorf("Failed to delete PodDisruptionBudget %s from cache: %v", pdb.Name, err)
		return
	}
	return
}

func (sc *SchedulerCache) PodInformer() clientv1.PodInformer {
	return sc.podInformer
}

func (sc *SchedulerCache) NodeInformer() clientv1.NodeInformer {
	return sc.nodeInformer
}

func (sc *SchedulerCache) QueueInformer() arbclient.QueueInformer {
	return sc.queueInformer
}

func (sc *SchedulerCache) PdbInformer() policyv1.PodDisruptionBudgetInformer {
	return sc.pdbInformer
}
