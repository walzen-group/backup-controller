package runs

import (
	"context"
	"fmt"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// scale sets a workload's replica count through its scale subresource, the
// only write the controller makes to a Deployment or a StatefulSet.
//
// Parameters:
//   - c is the client that reads and writes the subresource. The manager's
//     client sends subresource requests straight to the API server.
//   - object is an empty Deployment or StatefulSet that carries the
//     workload's namespace and name (see workloadObject). It is only read.
//   - replicas is the count to set: 0 to stop the workload, or the count
//     the run recorded to give it back.
//
// It returns nil once the API server has stored the count. A workload that
// does not exist is an error for which apierrors.IsNotFound is true, so a
// restart can skip it. Any other refusal is returned wrapped, with the
// workload's name and the count.
//
// It reads the Scale, sets spec.replicas, clears the resourceVersion and
// writes the Scale back with the controller's field manager. A Scale
// without a resourceVersion is an unconditional update, which both kinds
// allow (AllowUnconditionalUpdate in the Deployment and StatefulSet
// strategies of Kubernetes 1.36), so a status write by the workload's
// controller between the read and the write does not fail the scale. The
// API server copies only spec.replicas from the Scale into the object it
// holds, so no other field can be overwritten. The Scale keeps the UID it
// read, and a workload deleted and created again in between is a Conflict.
// The subresource can change nothing but the replica count, which is why
// the ClusterRole grants no write verb on the workloads themselves.
func scale(ctx context.Context, c client.Client, object client.Object, replicas int32) error {
	current := &autoscalingv1.Scale{}
	err := c.SubResource("scale").Get(ctx, object, current)
	if err == nil {
		current.Spec.Replicas = replicas
		current.ResourceVersion = ""
		err = c.SubResource("scale").Update(ctx, object, client.WithSubResourceBody(current), FieldOwner)
	}
	if err != nil {
		return fmt.Errorf("scale %s to %d: %w", object.GetName(), replicas, err)
	}
	return nil
}
