package conformance

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func TestApplyGangSchedulerAdapter(t *testing.T) {
	makeJob := func() *batchv1.Job {
		p := int32(2)
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       batchv1.JobSpec{Parallelism: &p},
		}
	}

	tests := []struct {
		name          string
		schedulerName string
		wantErr       bool
		errSubstring  string
	}{
		{
			name:          "kueue is a no-op",
			schedulerName: "kueue",
			wantErr:       false,
		},
		{
			name:          "empty string returns error",
			schedulerName: "",
			wantErr:       true,
			errSubstring:  "unsupported",
		},
		{
			name:          "invalid name returns error",
			schedulerName: "nonexistent-scheduler",
			wantErr:       true,
			errSubstring:  "unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore the global flag.
			original := *gangSchedulerName
			*gangSchedulerName = tt.schedulerName
			t.Cleanup(func() { *gangSchedulerName = original })

			job := makeJob()
			err := applyGangSchedulerAdapter(context.Background(), t, nil, job)

			if tt.wantErr && err == nil {
				t.Fatalf("expected error for scheduler name %q, got nil", tt.schedulerName)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error for scheduler name %q: %v", tt.schedulerName, err)
			}
			if tt.wantErr && tt.errSubstring != "" {
				if got := err.Error(); !strings.Contains(got, tt.errSubstring) {
					t.Errorf("error %q does not contain %q", got, tt.errSubstring)
				}
			}
		})
	}
}

func TestApplyVolcanoAdapter(t *testing.T) {
	podGroupGVR := schema.GroupVersionResource{
		Group:    "scheduling.volcano.sh",
		Version:  "v1beta1",
		Resource: "podgroups",
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme)

	parallelism := int32(3)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-volcano-job", Namespace: "default"},
		Spec:       batchv1.JobSpec{Parallelism: &parallelism},
	}

	original := *gangSchedulerName
	*gangSchedulerName = "volcano"
	t.Cleanup(func() { *gangSchedulerName = original })

	if err := applyGangSchedulerAdapter(context.Background(), t, dynamicClient, job); err != nil {
		t.Fatalf("applyGangSchedulerAdapter returned unexpected error: %v", err)
	}

	// Verify SchedulerName is set on the pod template.
	if job.Spec.Template.Spec.SchedulerName != "volcano" {
		t.Errorf("expected SchedulerName %q, got %q", "volcano", job.Spec.Template.Spec.SchedulerName)
	}

	// Verify both group-name annotations are set.
	for _, key := range []string{"scheduling.k8s.io/group-name", "scheduling.volcano.sh/group-name"} {
		if got := job.Spec.Template.Annotations[key]; got != job.Name {
			t.Errorf("annotation %q = %q, want %q", key, got, job.Name)
		}
	}

	// Verify PodGroup was created with the correct GVR and spec.minMember.
	pg, err := dynamicClient.Resource(podGroupGVR).Namespace(job.Namespace).Get(
		context.Background(), job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PodGroup not found: %v", err)
	}

	minMember, found, err := unstructured.NestedInt64(pg.Object, "spec", "minMember")
	if err != nil || !found {
		t.Fatalf("spec.minMember not found in PodGroup: err=%v found=%v", err, found)
	}
	if minMember != int64(parallelism) {
		t.Errorf("spec.minMember = %d, want %d", minMember, parallelism)
	}
}
