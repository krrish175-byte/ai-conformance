package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var (
	operatorNamespace        *string
	operatorLabelSelector    *string
	operatorWebhookName      *string
	operatorCRDName          *string
	operatorRuntimeName      *string
	operatorReadyTimeout     *time.Duration
	operatorReconcileTimeout *time.Duration
)

func init() {
	operatorNamespace = flag.String("operator-namespace", "kubeflow-system",
		"Namespace where the AI operator pods are running.")
	operatorLabelSelector = flag.String("operator-label-selector", "app.kubernetes.io/name=kubeflow-trainer",
		"Label selector to identify operator pods.")
	operatorWebhookName = flag.String("operator-webhook-name", "validator.trainer.kubeflow.org",
		"Name of the ValidatingWebhookConfiguration for the operator.")
	operatorCRDName = flag.String("operator-crd-name", "trainjobs.trainer.kubeflow.org",
		"Name of the CRD to verify is registered with the API server.")
	operatorRuntimeName = flag.String("operator-runtime-name", "torch-distributed",
		"Name of a ClusterTrainingRuntime to use for the valid TrainJob reconciliation test.")
	operatorReadyTimeout = flag.Duration("operator-ready-timeout", 5*time.Minute,
		"Timeout for operator pods to reach Running state.")
	operatorReconcileTimeout = flag.Duration("operator-reconcile-timeout", 3*time.Minute,
		"Timeout for the controller to reconcile a valid custom resource.")
}


var (
	crdGVR = schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	trainJobGVR = schema.GroupVersionResource{
		Group:    "trainer.kubeflow.org",
		Version:  "v1alpha1",
		Resource: "trainjobs",
	}

	clusterTrainingRuntimeGVR = schema.GroupVersionResource{
		Group:    "trainer.kubeflow.org",
		Version:  "v1alpha1",
		Resource: "clustertrainingruntimes",
	}
)

// TestRobustCRDControllerOperation verifies the Robust CRD and Controller Operation requirement.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/blob/main/kars/0063-robust-crd-controller-operation/README.md
func TestRobustCRDControllerOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	clientset := getClientset(t)
	dynamicClient := getDynamicClient(t)

	ctx := context.Background()


	namespace := randomNamespaceName("crd-controller")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create test namespace: %v", err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})

	t.Run("OperatorPodsRunning", func(t *testing.T) {
		testOperatorPodsRunning(ctx, t, clientset)
	})

	t.Run("CRDRegistered", func(t *testing.T) {
		testCRDRegistered(ctx, t, dynamicClient)
	})

	t.Run("WebhookOperational", func(t *testing.T) {
		testWebhookOperational(ctx, t, clientset)
	})

	t.Run("WebhookRejectsInvalidCR", func(t *testing.T) {
		testWebhookRejectsInvalidCR(ctx, t, dynamicClient, namespace)
	})

	t.Run("ControllerReconcilesCR", func(t *testing.T) {
		testControllerReconcilesCR(ctx, t, dynamicClient, namespace)
	})
}


func testOperatorPodsRunning(ctx context.Context, t *testing.T, clientset kubernetes.Interface) {
	t.Helper()
	t.Logf("Verifying operator pods are Running in namespace %s (selector: %s)...", *operatorNamespace, *operatorLabelSelector)

	var lastPodCount int
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, *operatorReadyTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(*operatorNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: *operatorLabelSelector,
		})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil

		if len(pods.Items) == 0 {
			lastPodCount = 0
			return false, nil
		}

		allReady := true
		for _, pod := range pods.Items {
			if !podIsReady(&pod) {
				allReady = false
				t.Logf("  Pod %s not ready (phase: %s)", pod.Name, pod.Status.Phase)
			}
		}
		lastPodCount = len(pods.Items)
		return allReady, nil
	})
	if err != nil {
		t.Fatalf("FAIL: Operator pods did not become Ready (found %d pods)%s: %v",
			lastPodCount, lastAPIErrorSuffix(lastAPIError), err)
	}
	t.Logf("PASS: %d operator pod(s) are Ready in namespace %s", lastPodCount, *operatorNamespace)
}


func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}


func testCRDRegistered(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface) {
	t.Helper()
	t.Logf("Verifying CRD %s is registered and Established...", *operatorCRDName)

	crd, err := dynamicClient.Resource(crdGVR).Get(ctx, *operatorCRDName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("FAIL: CRD %s is not registered with the API server: %v", *operatorCRDName, err)
	}

	if err := verifyCRDEstablished(crd); err != nil {
		t.Fatalf("FAIL: CRD %s is registered but not Established: %v", *operatorCRDName, err)
	}

	t.Logf("PASS: CRD %s is registered and Established", *operatorCRDName)
}


func verifyCRDEstablished(crd *unstructured.Unstructured) error {
	conditions, found, err := unstructured.NestedSlice(crd.Object, "status", "conditions")
	if err != nil {
		return fmt.Errorf("failed to read status.conditions: %w", err)
	}
	if !found || len(conditions) == 0 {
		return fmt.Errorf("CRD has no status conditions")
	}

	for _, c := range conditions {
		condition, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condition, "type")
		condStatus, _, _ := unstructured.NestedString(condition, "status")
		if condType == "Established" && condStatus == "True" {
			return nil
		}
	}
	return fmt.Errorf("Established=True condition not found")
}


func testWebhookOperational(ctx context.Context, t *testing.T, clientset kubernetes.Interface) {
	t.Helper()
	t.Logf("Verifying ValidatingWebhookConfiguration %s exists...", *operatorWebhookName)

	webhook, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(
		ctx, *operatorWebhookName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("FAIL: ValidatingWebhookConfiguration %s not found: %v", *operatorWebhookName, err)
	}

	if len(webhook.Webhooks) == 0 {
		t.Fatalf("FAIL: ValidatingWebhookConfiguration %s has no webhooks", *operatorWebhookName)
	}

	for _, wh := range webhook.Webhooks {
		if len(wh.Rules) == 0 {
			t.Errorf("FAIL: Webhook %s has no rules configured", wh.Name)
		}
		t.Logf("  Webhook: %s (rules: %d)", wh.Name, len(wh.Rules))
	}
	if t.Failed() {
		return
	}
	t.Logf("PASS: ValidatingWebhookConfiguration %s exists with %d webhook(s)", *operatorWebhookName, len(webhook.Webhooks))
}


func testWebhookRejectsInvalidCR(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace string) {
	t.Helper()
	t.Logf("Submitting invalid TrainJob to verify webhook rejection...")

	invalidJob := buildInvalidTrainJob(namespace, "invalid-trainjob")

	_, err := dynamicClient.Resource(trainJobGVR).Namespace(namespace).Create(ctx, invalidJob, metav1.CreateOptions{})
	if err == nil {

		t.Cleanup(func() {
			_ = dynamicClient.Resource(trainJobGVR).Namespace(namespace).Delete(ctx, "invalid-trainjob", metav1.DeleteOptions{})
		})
		t.Fatalf("FAIL: Invalid TrainJob was accepted by the API server; expected admission webhook to reject it")
	}


	if apierrors.IsForbidden(err) || isAdmissionDenied(err) {
		t.Logf("PASS: Webhook correctly rejected invalid TrainJob: %v", err)
	} else {
		t.Fatalf("FAIL: TrainJob creation failed with an unexpected error (expected admission rejection): %v", err)
	}
}


func testControllerReconcilesCR(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace string) {
	t.Helper()


	t.Logf("Verifying ClusterTrainingRuntime %s exists...", *operatorRuntimeName)
	_, err := dynamicClient.Resource(clusterTrainingRuntimeGVR).Get(ctx, *operatorRuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("FAIL: ClusterTrainingRuntime %s not found (ensure operator was installed with default runtimes): %v",
			*operatorRuntimeName, err)
	}

	jobName := "valid-trainjob"
	validJob := buildValidTrainJob(namespace, jobName, *operatorRuntimeName)

	t.Logf("Creating valid TrainJob %s referencing ClusterTrainingRuntime %s...", jobName, *operatorRuntimeName)
	_, err = dynamicClient.Resource(trainJobGVR).Namespace(namespace).Create(ctx, validJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("FAIL: Valid TrainJob was rejected: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up TrainJob %s...", jobName)
		err := dynamicClient.Resource(trainJobGVR).Namespace(namespace).Delete(ctx, jobName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Logf("Failed to clean up TrainJob %s: %v", jobName, err)
		}
	})

	t.Logf("Waiting for controller to reconcile TrainJob %s...", jobName)
	if err := waitForCRReconciled(ctx, t, dynamicClient, namespace, jobName); err != nil {
		t.Fatalf("FAIL: Controller did not reconcile TrainJob %s: %v", jobName, err)
	}

	t.Logf("PASS: Controller reconciled TrainJob %s", jobName)
}


func buildInvalidTrainJob(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "trainer.kubeflow.org/v1alpha1",
			"kind":       "TrainJob",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"runtimeRef": map[string]interface{}{
					"name":     "does-not-exist-runtime",
					"apiGroup": "trainer.kubeflow.org",
					"kind":     "ClusterTrainingRuntime",
				},
			},
		},
	}
}


func buildValidTrainJob(namespace, name, runtimeName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "trainer.kubeflow.org/v1alpha1",
			"kind":       "TrainJob",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"runtimeRef": map[string]interface{}{
					"name":     runtimeName,
					"apiGroup": "trainer.kubeflow.org",
					"kind":     "ClusterTrainingRuntime",
				},
			},
		},
	}
}


func waitForCRReconciled(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace, name string) error {
	t.Helper()

	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, *operatorReconcileTimeout, true, func(ctx context.Context) (bool, error) {
		cr, err := dynamicClient.Resource(trainJobGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil

		conditions, found, err := unstructured.NestedSlice(cr.Object, "status", "conditions")
		if err != nil || !found || len(conditions) == 0 {
			return false, nil
		}

		for _, c := range conditions {
			condition, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(condition, "type")
			condStatus, _, _ := unstructured.NestedString(condition, "status")
			condReason, _, _ := unstructured.NestedString(condition, "reason")
			t.Logf("  TrainJob condition: type=%s, status=%s, reason=%s", condType, condStatus, condReason)

			if condStatus == "True" && condType == "Failed" {
				return false, fmt.Errorf("TrainJob %s reached a terminal failed state", name)
			}
		}

		for _, c := range conditions {
			condition, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(condition, "type")
			condStatus, _, _ := unstructured.NestedString(condition, "status")
			if condStatus == "True" && condType != "Failed" {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("TrainJob %s had no status conditions within %s%s: %w",
			name, *operatorReconcileTimeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}


func isAdmissionDenied(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	msg := strings.ToLower(statusErr.ErrStatus.Message)
	return strings.Contains(msg, "denied the request") ||
		strings.Contains(msg, "admission webhook")
}
