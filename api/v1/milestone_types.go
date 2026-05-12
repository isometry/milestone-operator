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

// MilestoneSpec defines the desired state of Milestone.
type MilestoneSpec struct {
	// DependsOn is the ordered set of dependencies this Milestone
	// aggregates. All target resources are scoped to the Milestone's own
	// namespace.
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	DependsOn []DependencyRef `json:"dependsOn"`
}

// MilestoneStatus is the observed state of Milestone.
type MilestoneStatus struct {
	MilestoneStatusBase `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Total",type="integer",JSONPath=".status.summary.total"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.summary.current"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Milestone aggregates the kstatus of resources matching its dependencies
// within its own namespace, exposing a kstatus-compatible Ready condition.
type Milestone struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MilestoneSpec   `json:"spec"`
	Status MilestoneStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MilestoneList contains a list of Milestone.
type MilestoneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Milestone `json:"items"`
}
