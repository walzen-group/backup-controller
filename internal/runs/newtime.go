package runs

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// newTime returns a pointer to a copy of t, so a caller can set a *metav1.Time
// field from a function's result in one expression.
func newTime(t metav1.Time) *metav1.Time { return &t }
