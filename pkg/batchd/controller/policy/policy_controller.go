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

package policy

import (
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"

	schedcache "github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/cache"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/policy/framework"
)

type PolicyController struct {
	config *rest.Config
	// clientset  *clientset.Clientset
	// kubeclient *kubernetes.Clientset
	cache schedcache.Cache
	// podSets    *cache.FIFO
}

func NewPolicyController(config *rest.Config, schedulerName string) (*PolicyController, error) {
	policyController := &PolicyController{
		config: config,
		cache:  schedcache.New(config, schedulerName),
	}

	return policyController, nil
}

func (pc *PolicyController) Run(stopCh <-chan struct{}) {
	// Start cache for policy.
	go pc.cache.Run(stopCh)
	pc.cache.WaitForCacheSync(stopCh)

	go wait.Until(pc.runOnce, 20*time.Second, stopCh)
}

func (pc *PolicyController) runOnce() {
	glog.V(4).Infof("Start scheduling ...")
	defer glog.V(4).Infof("End scheduling ...")

	ssn := framework.OpenSession(pc.cache)
	defer framework.CloseSession(ssn)

	for _, action := range policy.ActionChain {
		glog.V(3).Infof("Executue action %s in Session %v",
			action.Name(), ssn.ID)
		action.Execute(ssn)
	}
}
