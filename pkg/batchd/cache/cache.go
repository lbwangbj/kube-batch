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

package cache

import (
	"fmt"
	"sync"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientv1 "k8s.io/client-go/informers/core/v1"
	policyv1 "k8s.io/client-go/informers/policy/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/api"
	arbclient "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/client/informers/v1"
)

type SchedulerCache struct {
	sync.Mutex

	podInformer   clientv1.PodInformer
	nodeInformer  clientv1.NodeInformer
	queueInformer arbclient.QueueInformer
	pdbInformer   policyv1.PodDisruptionBudgetInformer

	kubeclient *kubernetes.Clientset

	Nodes map[api.NodeID]*api.NodeInfo

	Queues map[api.QueueID]*api.QueueInfo
	Jobs   map[api.JobID]*api.JobInfo
	Tasks  map[api.TaskID]*api.TaskInfo

	Pdbs map[string]*api.PdbInfo
}

// New returns a Cache implementation.
func New(config *rest.Config, schedulerName string) Cache {
	return newSchedulerCache(config, schedulerName)
}

func (sc *SchedulerCache) Snapshot() *CacheSnapshot {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	snapshot := &CacheSnapshot{
		Nodes: make(map[api.NodeID]*api.NodeInfo),

		Tasks:  make(map[api.TaskID]*api.TaskInfo),
		Jobs:   make(map[api.JobID]*api.JobInfo),
		Queues: make(map[api.QueueID]*api.QueueInfo),
	}

	for _, val := range sc.Nodes {
		snapshot.Nodes[val.UID] = api.NewNodeInfo(val.Node)
	}

	for _, q := range sc.Queues {
		snapshot.Queues[q.UID] = q.Clone()
	}

	for _, q := range snapshot.Queues {
		for _, j := range q.Jobs {
			snapshot.Jobs[j.UID] = j
			for _, ts := range j.Tasks {
				for _, t := range ts {
					snapshot.Tasks[t.UID] = t
					if len(t.Node) != 0 {
						snapshot.Nodes[t.Node].AddPod(t)
					}
				}
			}
		}
	}

	return snapshot
}

func (sc *SchedulerCache) Bind(taskID api.TaskID, host api.NodeID) error {
	task := sc.Tasks[taskID]
	if task == nil {
		return fmt.Errorf("failed to find Task %v", taskID)
	}

	job := sc.Jobs[task.Job]
	if job == nil {

	}

	node := sc.Nodes[host]
	if node == nil {
		return fmt.Errorf("failed to find node %v", host)
	}

	p := task.Pod

	job.DeleteTaskInfo(task)
	task.Status = api.Binding
	job.AddTaskInfo(task)

	node.AddPod(task)

	return sc.kubeclient.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		Target: v1.ObjectReference{
			Kind: "Node",
			Name: node.Name,
		},
	})
}

func (sc *SchedulerCache) Evict(taskID api.TaskID) error {
	task := sc.Tasks[taskID]
	if task == nil {
		return fmt.Errorf("failed to find Task %v", taskID)
	}

	p := task.Pod

	// TODO(k82cn): it's better to use /eviction instead of delete to avoid race-condition.
	return sc.kubeclient.CoreV1().Pods(p.Namespace).Delete(p.Name, &metav1.DeleteOptions{})
}

// func (sc *SchedulerCache) String() string {
// 	sc.Mutex.Lock()
// 	defer sc.Mutex.Unlock()

// 	str := "Cache:\n"

// 	if len(sc.Nodes) != 0 {
// 		str = str + "Nodes:\n"
// 		for _, n := range sc.Nodes {
// 			str = str + fmt.Sprintf("\t %s: idle(%v) used(%v) allocatable(%v) pods(%d)\n",
// 				n.Name, n.Idle, n.Used, n.Allocatable, len(n.Pods))
// 			for index, p := range n.Pods {
// 				str = str + fmt.Sprintf("\t\t Pod[%s] uid(%s) owner(%s) name(%s) namespace(%s) nodename(%s) phase(%s) request(%v) pod(%v)\n",
// 					index, p.UID, p.Job, p.Pod.Name, p.Pod.Namespace, p.NodeName, p.Pod.Status.Phase, p.Resreq, p.Pod)
// 			}
// 		}
// 	}

// 	if len(sc.Queues) != 0 {
// 		str = str + "Queues:\n"
// 		for ck, c := range sc.Queues {
// 			str = str + fmt.Sprintf("\t Queue(%s) name(%s) namespace(%s) Jobs(%d) value(%v)\n",
// 				ck, c.Queue.Name, c.Queue.Namespace, len(c.Jobs), c.Queue)
// 			for k, ps := range c.Jobs {
// 				str = str + fmt.Sprintf("\t\t Jobs[%s] running(%d) pending(%d)\n",
// 					k, len(ps.Tasks[api.Running]), len(ps.Tasks[api.Pending]))
// 				for _, ts := range ps.Tasks {
// 					for j, p := range ts {
// 						str = str + fmt.Sprintf("\t\t Task[%s] uid(%s) owner(%s) name(%s) namespace(%s) nodename(%s) phase(%s) request(%v) pod(%v)\n",
// 							j, p.UID, p.Job, p.Pod.Name, p.Pod.Namespace, p.NodeName, p.Pod.Status.Phase, p.Resreq, p.Pod)
// 					}
// 				}
// 			}

// 		}
// 	}

// 	return str
// }
