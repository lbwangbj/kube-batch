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

// func fetchMatchNodeForPodSet(psi *podSetInfo, nodes map[string]*api.NodeInfo) []*api.NodeInfo {
// 	matchNodes := make([]*api.NodeInfo, 0)
// 	for _, node := range nodes {
// 		if podSetMatchesNodeLabels(psi, node.Node) {
// 			matchNodes = append(matchNodes, node)
// 		}
// 	}

// 	return matchNodes
// }

// // The pod in PodSet can only schedule onto nodes that satisfy requirements in both NodeAffinity and nodeSelector.
// func podSetMatchesNodeLabels(psi *podSetInfo, node *v1.Node) bool {
// 	// Check if node.Labels match pod.Spec.NodeSelector.
// 	if len(psi.podSet.NodeSelector) > 0 {
// 		selector := labels.SelectorFromSet(labels.Set(psi.podSet.NodeSelector))
// 		if !selector.Matches(labels.Set(node.Labels)) {
// 			return false
// 		}
// 	}

// 	return true
// }
