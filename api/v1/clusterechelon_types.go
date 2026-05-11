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

// ClusterEchelonSpec defines the desired state of ClusterEchelon.
//
// +kubebuilder:validation:XValidation:rule="self.members.all(k, k.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'))",message="member keys must be RFC-1123 labels"
type ClusterEchelonSpec struct {
	// Members is the set of resource selections this ClusterEchelon aggregates
	// over, keyed by user-given names. Each member may scope its search via
	// Namespaces or NamespaceSelector.
	// +kubebuilder:validation:MinProperties=1
	Members map[string]ClusterMemberSpec `json:"members"`
}

// ClusterEchelonStatus is the observed state of ClusterEchelon.
type ClusterEchelonStatus struct {
	EchelonStatusBase `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cech
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Total",type="integer",JSONPath=".status.summary.total"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.summary.current"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ClusterEchelon aggregates the kstatus of resources matching its members
// across namespaces, exposing a kstatus-compatible Ready condition.
type ClusterEchelon struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterEchelonSpec   `json:"spec"`
	Status ClusterEchelonStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterEchelonList contains a list of ClusterEchelon.
type ClusterEchelonList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterEchelon `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterEchelon{}, &ClusterEchelonList{})
}
