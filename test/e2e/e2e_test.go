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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/isometry/milestone-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "milestone-operator-system"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "milestone-operator-controller-manager-metrics-service"

var _ = Describe("Manager", Ordered, func() {
	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("undeploying the controller-manager")
		cmd := exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", "-l", "control-plane=controller-manager",
				"-n", namespace, "--tail=-1")
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pods", "-l", "control-plane=controller-manager",
				"-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				podName := getControllerPodName(g)

				// Validate the pod's status
				cmd := exec.Command("kubectl", "get",
					"pods", podName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			// Metrics is plain HTTP on :8080 (--metrics-secure=false), so scrape it from the
			// host through the API server service proxy — no in-cluster curl pod, bearer token,
			// or metrics-reader binding needed. The Eventually absorbs the pod readiness window:
			// the proxy returns a no-endpoints error until the readiness probe (readyz) passes.
			By("scraping /metrics via the API server service proxy")
			proxyPath := fmt.Sprintf(
				"/api/v1/namespaces/%s/services/http:%s:8080/proxy/metrics",
				namespace, metricsServiceName)
			var body string
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "--raw", proxyPath))
				g.Expect(err).NotTo(HaveOccurred())
				body = out
			}, 2*time.Minute, time.Second).Should(Succeed())
			Expect(body).To(ContainSubstring("controller_runtime_reconcile_total"))
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should reach Ready=True when its dependencies converge", func() {
			testNS := "e2e-ready"
			By("creating the test namespace")
			cmd := exec.Command("kubectl", "create", "ns", testNS)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "create test namespace")
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "ns", testNS, "--wait=false")
				_, _ = utils.Run(cmd)
			})

			By("applying three labelled ConfigMaps")
			Expect(kubectlApplyYAML(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-a
  namespace: ` + testNS + `
  labels:
    milestone-test: ready
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-b
  namespace: ` + testNS + `
  labels:
    milestone-test: ready
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-c
  namespace: ` + testNS + `
  labels:
    milestone-test: ready
`)).To(Succeed())

			By("applying a Milestone that targets the ConfigMaps")
			Expect(kubectlApplyYAML(`
apiVersion: milestone.as-code.io/v1
kind: Milestone
metadata:
  name: ready-test
  namespace: ` + testNS + `
spec:
  dependsOn:
    - name: configmaps
      emptySetPolicy: NotReady
      target:
        kind: ConfigMap
        selector:
          matchLabels:
            milestone-test: ready
`)).To(Succeed())

			By("expecting Ready=True and Summary.Total == 3")
			Eventually(func(g Gomega) {
				status, err := utils.Run(exec.Command("kubectl", "get", "milestone", "ready-test",
					"-n", testNS,
					"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "Ready condition")

				total, err := utils.Run(exec.Command("kubectl", "get", "milestone", "ready-test",
					"-n", testNS, "-o", "jsonpath={.status.summary.total}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(total).To(Equal("3"), "Summary.Total")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should clear Stalled and converge after a late CRD install", func() {
			testNS := "e2e-late-crd"
			By("creating the test namespace")
			cmd := exec.Command("kubectl", "create", "ns", testNS)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "create test namespace")
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "ns", testNS, "--wait=false")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "crd", "lates.late.example.com", "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			By("applying a Milestone that targets a not-yet-installed CRD")
			Expect(kubectlApplyYAML(`
apiVersion: milestone.as-code.io/v1
kind: Milestone
metadata:
  name: late-test
  namespace: ` + testNS + `
spec:
  dependsOn:
    - name: lates
      emptySetPolicy: Unknown
      target:
        group: late.example.com
        kind: Late
`)).To(Succeed())

			By("expecting Stalled=True before the CRD exists")
			Eventually(func(g Gomega) {
				status, err := utils.Run(exec.Command("kubectl", "get", "milestone", "late-test",
					"-n", testNS,
					"-o", `jsonpath={.status.conditions[?(@.type=="Stalled")].status}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "Stalled condition")
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("installing the Late CRD")
			Expect(kubectlApplyYAML(`
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: lates.late.example.com
spec:
  group: late.example.com
  scope: Namespaced
  names:
    plural: lates
    singular: late
    kind: Late
    listKind: LateList
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          x-kubernetes-preserve-unknown-fields: true
      subresources:
        status: {}
`)).To(Succeed())

			By("applying a matching Late resource")
			Expect(kubectlApplyYAML(`
apiVersion: late.example.com/v1
kind: Late
metadata:
  name: late-1
  namespace: ` + testNS + `
`)).To(Succeed())

			By("expecting Stalled to clear and Ready to converge")
			Eventually(func(g Gomega) {
				stalled, err := utils.Run(exec.Command("kubectl", "get", "milestone", "late-test",
					"-n", testNS,
					"-o", `jsonpath={.status.conditions[?(@.type=="Stalled")].status}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(stalled).NotTo(Equal("True"), "Stalled should clear")

				ready, err := utils.Run(exec.Command("kubectl", "get", "milestone", "late-test",
					"-n", testNS,
					"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "Ready condition")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should poke the Flux parent on Ready transition", func() {
			testNS := "e2e-flux-poke"
			By("installing the stub Kustomization CRD")
			cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/kustomization-crd.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "install stub Kustomization CRD")
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "ns", testNS, "--wait=false")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/kustomization-crd.yaml", "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			By("waiting for the CRD to become Established")
			Eventually(func(g Gomega) {
				status, err := utils.Run(exec.Command("kubectl", "get", "crd",
					"kustomizations.kustomize.toolkit.fluxcd.io",
					"-o", `jsonpath={.status.conditions[?(@.type=="Established")].status}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "CRD Established")
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			By("creating the test namespace")
			cmd = exec.Command("kubectl", "create", "ns", testNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "create test namespace")

			By("applying a parent Kustomization and a matching ConfigMap")
			Expect(kubectlApplyYAML(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: parent
  namespace: ` + testNS + `
spec:
  interval: 5m
  prune: true
  sourceRef:
    kind: GitRepository
    name: noop
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-flux
  namespace: ` + testNS + `
  labels:
    milestone-test: flux-poke
`)).To(Succeed())

			By("applying a child Milestone labelled with the Flux parent reference")
			Expect(kubectlApplyYAML(`
apiVersion: milestone.as-code.io/v1
kind: Milestone
metadata:
  name: child
  namespace: ` + testNS + `
  labels:
    kustomize.toolkit.fluxcd.io/name: parent
    kustomize.toolkit.fluxcd.io/namespace: ` + testNS + `
spec:
  dependsOn:
    - name: configmaps
      emptySetPolicy: NotReady
      target:
        kind: ConfigMap
        selector:
          matchLabels:
            milestone-test: flux-poke
`)).To(Succeed())

			By("expecting the parent Kustomization to carry reconcile.fluxcd.io/requestedAt")
			Eventually(func(g Gomega) {
				ts, err := utils.Run(exec.Command("kubectl", "get", "kustomization", "parent",
					"-n", testNS,
					"-o", `jsonpath={.metadata.annotations.reconcile\.fluxcd\.io/requestedAt}`))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ts).NotTo(BeEmpty(), "reconcile.fluxcd.io/requestedAt annotation should be set")
				_, perr := time.Parse(time.RFC3339Nano, ts)
				g.Expect(perr).NotTo(HaveOccurred(), "annotation should be a valid RFC3339Nano timestamp")
			}, 1*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should grow Summary.Total when a namespace gains the selector label", func() {
			nsA := "e2e-cm-evol-a"
			nsB := "e2e-cm-evol-b"
			cmName := "evol-test"

			By("creating two namespaces, one labelled tier=platform")
			Expect(kubectlApplyYAML(`
apiVersion: v1
kind: Namespace
metadata:
  name: ` + nsA + `
  labels:
    tier: platform
---
apiVersion: v1
kind: Namespace
metadata:
  name: ` + nsB + `
`)).To(Succeed())
			DeferCleanup(func() {
				for _, ns := range []string{nsA, nsB} {
					cmd := exec.Command("kubectl", "delete", "ns", ns, "--wait=false")
					_, _ = utils.Run(cmd)
				}
				cmd := exec.Command("kubectl", "delete", "clustermilestone", cmName, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			By("applying matching ConfigMaps in both namespaces")
			Expect(kubectlApplyYAML(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-a
  namespace: ` + nsA + `
  labels:
    milestone-test: evol
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-b
  namespace: ` + nsB + `
  labels:
    milestone-test: evol
`)).To(Succeed())

			By("applying a ClusterMilestone scoped by namespaceSelector tier=platform")
			Expect(kubectlApplyYAML(`
apiVersion: milestone.as-code.io/v1
kind: ClusterMilestone
metadata:
  name: ` + cmName + `
spec:
  dependsOn:
    - name: configmaps
      emptySetPolicy: NotReady
      target:
        kind: ConfigMap
        selector:
          matchLabels:
            milestone-test: evol
        namespaceSelector:
          matchLabels:
            tier: platform
`)).To(Succeed())

			By("expecting Summary.Total == 1 (only namespace a counts)")
			Eventually(func(g Gomega) {
				total, err := utils.Run(exec.Command("kubectl", "get", "clustermilestone", cmName,
					"-o", "jsonpath={.status.summary.total}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(total).To(Equal("1"), "Summary.Total pre-label")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("labelling namespace b with tier=platform")
			cmd := exec.Command("kubectl", "label", "ns", nsB, "tier=platform", "--overwrite")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "label namespace b")

			By("expecting Summary.Total to grow to 2")
			Eventually(func(g Gomega) {
				total, err := utils.Run(exec.Command("kubectl", "get", "clustermilestone", cmName,
					"-o", "jsonpath={.status.summary.total}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(total).To(Equal("2"), "Summary.Total post-label")
			}, 1*time.Minute, 2*time.Second).Should(Succeed())
		})
	})
})

// getControllerPodName resolves the single controller-manager pod by label. It takes a Gomega so
// that a transient "0 pods" window during rollout retries within an Eventually rather than failing.
func getControllerPodName(g Gomega) string {
	cmd := exec.Command("kubectl", "get",
		"pods", "-l", "control-plane=controller-manager",
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
		"-n", namespace,
	)
	podOutput, err := utils.Run(cmd)
	g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
	podNames := utils.GetNonEmptyLines(podOutput)
	g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
	g.Expect(podNames[0]).To(ContainSubstring("controller-manager"))
	return podNames[0]
}

// kubectlApplyYAML pipes the provided YAML manifest to `kubectl apply -f -`.
func kubectlApplyYAML(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}
