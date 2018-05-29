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

package framework

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/uuid"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/batchd/cache"
)

type PluginBuilder func() Plugin

var mutex sync.Mutex
var pluginBuidlers []PluginBuilder

func OpenSession(cache cache.Cache) *Session {
	snapshot := cache.Snapshot()

	ssn := &Session{
		ID:     uuid.NewUUID(),
		cache:  cache,
		Tasks:  snapshot.Tasks,
		Jobs:   snapshot.Jobs,
		Queues: snapshot.Queues,
		Nodes:  snapshot.Nodes,
	}

	for _, pb := range pluginBuidlers {
		p := pb()
		p.OnSessionOpen(ssn)
		ssn.plugins[p.Name()] = p
	}

	return ssn
}

func CloseSession(ssn *Session) {
	for _, p := range ssn.plugins {
		p.OnSessionClose(ssn)
	}

	ssn.Queues = nil
	ssn.Nodes = nil

	ssn.cache = nil
	ssn.plugins = nil
}

func RegisterPluginBuilder(pb PluginBuilder) {
	mutex.Lock()
	defer mutex.Unlock()

	pluginBuidlers = append(pluginBuidlers, pb)
}
