/*
Copyright 2026.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterMilestoneSpec defines the desired state of ClusterMilestone.
type ClusterMilestoneSpec struct {
	// DependsOn is the ordered set of dependencies this ClusterMilestone
	// aggregates. Each dependency may scope its search via
	// Target.Namespaces or Target.NamespaceSelector.
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	DependsOn []ClusterDependencyRef `json:"dependsOn"`
}

// ClusterMilestoneStatus is the observed state of ClusterMilestone.
type ClusterMilestoneStatus struct {
	MilestoneStatusBase `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Total",type="integer",JSONPath=".status.summary.total"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.summary.current"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ClusterMilestone aggregates the kstatus of resources matching its
// dependencies across namespaces, exposing a kstatus-compatible Ready
// condition.
type ClusterMilestone struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterMilestoneSpec   `json:"spec"`
	Status ClusterMilestoneStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterMilestoneList contains a list of ClusterMilestone.
type ClusterMilestoneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterMilestone `json:"items"`
}
