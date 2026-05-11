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

// EchelonSpec defines the desired state of Echelon.
//
// +kubebuilder:validation:XValidation:rule="self.members.all(k, k.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'))",message="member keys must be RFC-1123 labels"
type EchelonSpec struct {
	// Members is the set of resource selections this Echelon aggregates over,
	// keyed by user-given names. All resources are scoped to the Echelon's
	// own namespace.
	// +kubebuilder:validation:MinProperties=1
	Members map[string]MemberSpec `json:"members"`
}

// EchelonStatus is the observed state of Echelon.
type EchelonStatus struct {
	EchelonStatusBase `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=ech
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Total",type="integer",JSONPath=".status.summary.total"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.summary.current"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Echelon aggregates the kstatus of resources matching its members within its
// own namespace, exposing a kstatus-compatible Ready condition.
type Echelon struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EchelonSpec   `json:"spec"`
	Status EchelonStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EchelonList contains a list of Echelon.
type EchelonList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Echelon `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Echelon{}, &EchelonList{})
}
