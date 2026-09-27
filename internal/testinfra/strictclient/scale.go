package strictclient

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// scaleSubresource is the name of the subresource that reads and writes a
// workload's replica count.
const scaleSubresource = "scale"

// SubResource returns a client for one subresource of an object.
//
// Parameters:
//   - name is the subresource, such as "scale" or "status".
//
// For "scale" it returns a client that serves Get and Update of the Scale of
// a Deployment or StatefulSet the way kube-apiserver 1.36.3 does (see
// scaleClient). Every other subresource, and Create and Patch of the Scale,
// go to the fake client unchanged.
func (c *Client) SubResource(name string) client.SubResourceClient {
	inner := c.WithWatch.SubResource(name)
	if name != scaleSubresource {
		return inner
	}
	return &scaleClient{SubResourceClient: inner, c: c}
}

// scaleClient serves the scale subresource of Deployments and StatefulSets.
//
// The fake client's own scale Update applies the Scale to the object the
// caller passes and stores that whole object, so an object that carries
// only a name loses its spec, the Scale's resourceVersion is ignored, and
// the generation stays as it was. kube-apiserver instead reads the stored
// object, builds its Scale, takes spec.replicas and the resourceVersion from
// the request's Scale, and stores the object through the workload's own
// registry store, so the update strategy raises the generation and a stale
// resourceVersion is a Conflict
// (k8s.io/kubernetes@v1.36.3/pkg/registry/apps/deployment/storage/storage.go:306-339
// and :405-480, statefulset/storage/storage.go:196-229 and :260-369).
type scaleClient struct {
	client.SubResourceClient

	c *Client
}

// Get reads the Scale of a stored workload.
//
// Parameters:
//   - obj names the workload; its kind picks the Deployment or StatefulSet
//     store. It is left as the caller passed it, as the real client leaves
//     it.
//   - subResource receives the Scale; it must be an *autoscalingv1.Scale.
//
// It returns NotFound when the workload does not exist, and a BadRequest
// when subResource is not a Scale.
func (s *scaleClient) Get(ctx context.Context, obj, subResource client.Object, _ ...client.SubResourceGetOption) error {
	out, ok := subResource.(*autoscalingv1.Scale)
	if !ok {
		return apierrors.NewBadRequest(fmt.Sprintf("expected Scale, got %T", subResource))
	}
	stored, err := s.c.stored(ctx, obj)
	if err != nil {
		return err
	}
	scale, err := scaleOf(stored)
	if err != nil {
		return err
	}
	*out = *scale
	return nil
}

// Update sets a stored workload's spec.replicas from a Scale.
//
// Parameters:
//   - obj names the workload. Only its kind, namespace and name are used,
//     so every other field of the stored object stays as it is.
//   - opts carry the Scale as client.WithSubResourceBody, and the update
//     options (field manager, dry run) the write passes on.
//
// It returns NotFound when the workload does not exist, a Conflict when the
// Scale carries a resourceVersion or uid other than the stored one, an
// Invalid error for a negative replica count, and a BadRequest when the
// body is not a Scale. On success the body holds the Scale of the stored
// object, with its new resourceVersion.
func (s *scaleClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	o := &client.SubResourceUpdateOptions{}
	o.ApplyOptions(opts)
	body, ok := o.SubResourceBody.(*autoscalingv1.Scale)
	if !ok {
		return apierrors.NewBadRequest(fmt.Sprintf("expected Scale, got %T", o.SubResourceBody))
	}
	stored, err := s.c.stored(ctx, obj)
	if err != nil {
		return err
	}
	if body.Spec.Replicas < 0 {
		path := field.NewPath("spec", "replicas")
		return apierrors.NewInvalid(schema.GroupKind{Group: "autoscaling", Kind: "Scale"}, obj.GetName(),
			field.ErrorList{field.Invalid(path, body.Spec.Replicas, "must be greater than or equal to 0")})
	}
	if err := setReplicas(stored, body.Spec.Replicas); err != nil {
		return err
	}
	stored.SetResourceVersion(body.ResourceVersion)
	if body.UID != "" {
		stored.SetUID(body.UID)
	}
	if err := s.c.Update(ctx, stored, &o.UpdateOptions); err != nil {
		return err
	}
	scale, err := scaleOf(stored)
	if err != nil {
		return err
	}
	*body = *scale
	return nil
}

// scaleOf builds the Scale kube-apiserver serves for a stored Deployment or
// StatefulSet: its name, namespace, uid, resourceVersion and
// creationTimestamp, spec.replicas, status.replicas and the selector as a
// string. A nil spec.replicas reads as 1, the value the API server's
// defaulting stores. It returns an error for any other kind, and a
// BadRequest for a selector that does not parse.
func scaleOf(obj client.Object) (*autoscalingv1.Scale, error) {
	var replicas *int32
	var statusReplicas int32
	var selector *metav1.LabelSelector
	switch o := obj.(type) {
	case *appsv1.Deployment:
		replicas, statusReplicas, selector = o.Spec.Replicas, o.Status.Replicas, o.Spec.Selector
	case *appsv1.StatefulSet:
		replicas, statusReplicas, selector = o.Spec.Replicas, o.Status.Replicas, o.Spec.Selector
	default:
		return nil, fmt.Errorf("strictclient: no scale subresource for %T", obj)
	}
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:              obj.GetName(),
			Namespace:         obj.GetNamespace(),
			UID:               obj.GetUID(),
			ResourceVersion:   obj.GetResourceVersion(),
			CreationTimestamp: obj.GetCreationTimestamp(),
		},
		Spec:   autoscalingv1.ScaleSpec{Replicas: 1},
		Status: autoscalingv1.ScaleStatus{Replicas: statusReplicas, Selector: parsed.String()},
	}
	if replicas != nil {
		scale.Spec.Replicas = *replicas
	}
	return scale, nil
}

// setReplicas sets spec.replicas on a Deployment or StatefulSet and returns
// an error for any other kind.
func setReplicas(obj client.Object, replicas int32) error {
	switch o := obj.(type) {
	case *appsv1.Deployment:
		o.Spec.Replicas = &replicas
	case *appsv1.StatefulSet:
		o.Spec.Replicas = &replicas
	default:
		return fmt.Errorf("strictclient: no scale subresource for %T", obj)
	}
	return nil
}
