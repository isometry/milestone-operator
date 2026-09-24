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

package e2e

import (
	"fmt"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/isometry/milestone-operator/test/utils"
)

// projectImage is the name of the image which will be build and loaded
// with the code source changes to be tested.
var projectImage = "example.com/milestone-operator:0.0.1"

// TestE2E runs the end-to-end (e2e) test suite for the project. These tests execute in an isolated,
// temporary environment to validate project changes with the purposed to be used in CI jobs.
// The default setup requires Kind; it builds the manager image with ko and loads it into Kind.
// KUBECONFIG must name the dedicated kind kubeconfig `make test-e2e` writes
// (bin/e2e.kubeconfig); anything else is refused before the first command.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting milestone-operator integration test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	// Before any command: the specs install CRDs, deploy the manager and
	// delete namespaces, so a hand-run `go test ./test/e2e/` must not fall
	// through to whatever cluster the ambient kubeconfig names. This repeats
	// the Makefile's e2e-guard on purpose, and every utils.Run child gets
	// only the kubeconfig vetted here.
	By("refusing any target other than the dedicated e2e kind cluster")
	Expect(utils.PinKubeconfig()).To(Succeed(),
		"run `make test-e2e`, which provisions its own kind cluster and kubeconfig")

	By("building the manager(Operator) image with ko")
	cmd := exec.Command("make", "ko-build-local", fmt.Sprintf("IMG=%s", projectImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager(Operator) image")

	// TODO(user): If you want to change the e2e test vendor from Kind, ensure the image is
	// built and available before running the tests. Also, remove the following block.
	By("loading the manager(Operator) image on Kind")
	err = utils.LoadImageToKindClusterWithName(projectImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager(Operator) image into Kind")
})
