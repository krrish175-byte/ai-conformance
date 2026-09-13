package conformance

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
				if got := err.Error(); !containsSubstring(got, tt.errSubstring) {
					t.Errorf("error %q does not contain %q", got, tt.errSubstring)
				}
			}
		})
	}
}

// Note: The "volcano" case requires a dynamic client and is tested via
// integration tests rather than unit tests.

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsIndex(s, sub))
}

func containsIndex(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
