/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"kubevirt.io/virt-template-engine/template"
	"kubevirt.io/virt-template/internal/logs"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	virtv1 "kubevirt.io/api/core/v1"

	templateapi "kubevirt.io/virt-template-api/core"
	"kubevirt.io/virt-template-api/core/v1alpha1"
)

const (
	failedToExtractDVT = "failed to extract DataVolumeTemplate(s)"
)

// VirtualMachineTemplateReconciler reconciles a VirtualMachineTemplate object
type VirtualMachineTemplateReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=template.kubevirt.io,resources=virtualmachinetemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=template.kubevirt.io,resources=virtualmachinetemplates/status,verbs=get;patch
// +kubebuilder:rbac:groups=template.kubevirt.io,resources=virtualmachinetemplates/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *VirtualMachineTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	tpl := &v1alpha1.VirtualMachineTemplate{}
	if err := r.Get(ctx, req.NamespacedName, tpl); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Unable to fetch VirtualMachineTemplate")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !tpl.DeletionTimestamp.IsZero() {
		log.Info("Skipping VirtualMachineTemplate that is being deleted")
		return ctrl.Result{}, nil
	}

	helper, err := patch.NewHelper(tpl, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	decodingErr := false
	defer func() {
		setTemplateStatusConditions(ctx, tpl, decodingErr)
		retErr = errors.Join(retErr, helper.Patch(ctx, tpl))
	}()

	tpl.Status.DataVolumeTemplateStatuses, decodingErr, err = r.checkDVTSources(ctx, tpl)

	return ctrl.Result{}, err
}

// checkDVTSources extracts DataVolumeTemplates from the embedded VM
// and checks whether their PVC/VolumeSnapshot sources exist and are ready.
func (r *VirtualMachineTemplateReconciler) checkDVTSources(
	ctx context.Context,
	tpl *v1alpha1.VirtualMachineTemplate,
) ([]v1alpha1.DataVolumeTemplateStatus, bool, error) {
	dvts, decodingErr := extractDVTs(ctx, tpl)
	if len(dvts) == 0 {
		return nil, decodingErr, nil
	}

	var (
		statuses []v1alpha1.DataVolumeTemplateStatus
		errs     []error
	)
	for _, dvt := range dvts {
		status, err := r.checkDVTSource(ctx, dvt)
		statuses = append(statuses, status)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return statuses, decodingErr, errors.Join(errs...)
}

// checkDVTSource checks a single DataVolumeTemplate's source and returns its status.
func (r *VirtualMachineTemplateReconciler) checkDVTSource(
	ctx context.Context,
	dvt virtv1.DataVolumeTemplateSpec,
) (v1alpha1.DataVolumeTemplateStatus, error) {
	status := v1alpha1.DataVolumeTemplateStatus{
		Name:  dvt.Name,
		Ready: true,
	}

	if dvt.Spec.Source == nil {
		status.Ready = false
		status.Message = "source is missing"
		return status, nil
	}

	source := dvt.Spec.Source
	switch {
	case source.PVC != nil:
		status.SourceType = "PVC"
		status.SourceNamespace = source.PVC.Namespace
		status.SourceName = source.PVC.Name
	case source.Snapshot != nil:
		status.SourceType = "VolumeSnapshot"
		status.SourceNamespace = source.Snapshot.Namespace
		status.SourceName = source.Snapshot.Name
	case source.HTTP != nil:
		status.SourceType = "HTTP"
	case source.Registry != nil:
		status.SourceType = "Registry"
	case source.S3 != nil:
		status.SourceType = "S3"
	case source.GCS != nil:
		status.SourceType = "GCS"
	case source.Blank != nil:
		status.SourceType = "Blank"
	case source.Upload != nil:
		status.SourceType = "Upload"
	case source.Imageio != nil:
		status.SourceType = "Imageio"
	case source.VDDK != nil:
		status.SourceType = "VDDK"
	default:
		status.SourceType = "Unknown"
	}

	if status.SourceType != "PVC" && status.SourceType != "VolumeSnapshot" {
		return status, nil
	}

	if status.SourceNamespace == "" || status.SourceName == "" {
		status.Ready = false
		status.Message = "Source namespace or name is missing"
		return status, nil
	}

	if containsParameterReference(status.SourceNamespace) || containsParameterReference(status.SourceName) {
		status.Ready = true
		status.Message = "Source contains parameter reference(s)"
		return status, nil
	}

	var err error
	switch status.SourceType {
	case "PVC":
		status.Ready, status.Message, err = r.checkPVCStatus(ctx, status.SourceNamespace, status.SourceName)
	case "VolumeSnapshot":
		status.Ready, status.Message, err = r.checkVolumeSnapshotStatus(ctx, status.SourceNamespace, status.SourceName)
	}

	return status, err
}

func (r *VirtualMachineTemplateReconciler) checkPVCStatus(ctx context.Context, namespace, name string) (bool, string, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pvc); k8serrors.IsNotFound(err) {
		return false, "PVC not found", err
	} else if err != nil {
		return false, "Failed to get PVC", err
	}
	if pvc.Status.Phase == corev1.ClaimBound {
		return true, "PVC is bound", nil
	}
	return false, fmt.Sprintf("PVC is in phase %s", pvc.Status.Phase), nil
}

func (r *VirtualMachineTemplateReconciler) checkVolumeSnapshotStatus(ctx context.Context, namespace, name string) (bool, string, error) {
	snap := &snapshotv1.VolumeSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, snap); k8serrors.IsNotFound(err) {
		return false, "VolumeSnapshot not found", err
	} else if err != nil {
		return false, "Failed to get VolumeSnapshot", err
	}
	if snap.Status != nil && snap.Status.ReadyToUse != nil && *snap.Status.ReadyToUse {
		return true, "VolumeSnapshot is ready to use", nil
	}
	return false, "VolumeSnapshot is not ready to use", nil
}

func extractDVTs(ctx context.Context, tpl *v1alpha1.VirtualMachineTemplate) ([]virtv1.DataVolumeTemplateSpec, bool) {
	log := logf.FromContext(ctx)

	if tpl.Spec.VirtualMachine == nil {
		return nil, true
	}

	obj, err := template.GetVirtualMachineObject(&tpl.Spec)
	if err != nil {
		log.Error(err, "failed to get VirtualMachineObject")
		return nil, false
	}

	switch typedObj := obj.(type) {
	case *virtv1.VirtualMachine:
		return typedObj.Spec.DataVolumeTemplates, true
	case *unstructured.Unstructured:
		return extractDVTsFromUnstructured(ctx, typedObj.Object)
	default:
		log.V(logs.DebugLevel).Info(fmt.Sprintf("unable to convert into DataVolumeTemplates: object is %T", typedObj))
	}

	return nil, false
}

func extractDVTsFromUnstructured(ctx context.Context, obj map[string]any) ([]virtv1.DataVolumeTemplateSpec, bool) {
	log := logf.FromContext(ctx)

	dvtsRaw, found, err := unstructured.NestedSlice(obj, "spec", "dataVolumeTemplates")
	if err != nil {
		log.V(logs.DebugLevel).Error(err, failedToExtractDVT)
		return nil, false
	}
	if !found {
		return nil, true
	}

	var dvts []virtv1.DataVolumeTemplateSpec
	decodingErr := false
	for _, dvtRaw := range dvtsRaw {
		dvtMap, ok := dvtRaw.(map[string]interface{})
		if !ok {
			log.V(logs.DebugLevel).Info(failedToExtractDVT)
			decodingErr = true
			continue
		}
		var dvt virtv1.DataVolumeTemplateSpec
		if err := runtime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(dvtMap, &dvt, true); err != nil {
			log.V(logs.DebugLevel).Error(err, failedToExtractDVT)
			decodingErr = true
			continue
		}
		if dvt.Name == "" {
			log.V(logs.DebugLevel).Info("DataVolumeTemplate name cannot be empty")
			decodingErr = true
			continue
		}
		dvts = append(dvts, dvt)
	}

	return dvts, decodingErr
}

func containsParameterReference(s string) bool {
	return strings.Contains(s, "${")
}

func setTemplateStatusConditions(ctx context.Context, tpl *v1alpha1.VirtualMachineTemplate, decodingErr bool) {
	if allReady {
		meta.SetStatusCondition(&tpl.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: tpl.Generation,
			Reason:             v1alpha1.ReasonReconciled,
			Message:            "VirtualMachineTemplate is ready to be processed",
		})
	} else {
		var notReadyNames []string
		for _, status := range dvtStatuses {
			if !status.Ready {
				notReadyNames = append(notReadyNames, status.Name)
			}
		}
		meta.SetStatusCondition(&tpl.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: tpl.Generation,
			Reason:             v1alpha1.ReasonDataVolumeTemplateNotReady,
			Message:            fmt.Sprintf("DataVolumeTemplate sources not ready: %s", strings.Join(notReadyNames, ", ")),
		})
	}

	if retErr != nil {
		logf.FromContext(ctx).Error(retErr, "Reconciliation failed")
		setTemplateRequestReadyCondition(ctx, tplReq, metav1.ConditionFalse, v1alpha1.ReasonFailed, "%s", retErr.Error())
		if meta.FindStatusCondition(tplReq.Status.Conditions, v1alpha1.ConditionProgressing) == nil {
			setTemplateRequestProgressingCondition(ctx, tplReq, metav1.ConditionTrue, v1alpha1.ReasonReconciling)
		}

		return
	}

	progressingStatus := metav1.ConditionTrue
	progressingReason := v1alpha1.ReasonReconciling

	cond := meta.FindStatusCondition(tplReq.Status.Conditions, v1alpha1.ConditionReady)
	if cond == nil {
		setTemplateRequestReadyCondition(ctx, tplReq, metav1.ConditionFalse, v1alpha1.ReasonReconciling, "")
	} else if cond.Status == metav1.ConditionTrue {
		progressingStatus = metav1.ConditionFalse
		progressingReason = v1alpha1.ReasonReconciled
	}

	if meta.FindStatusCondition(tplReq.Status.Conditions, v1alpha1.ConditionProgressing) == nil {
		setTemplateRequestProgressingCondition(ctx, tplReq, progressingStatus, progressingReason)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.VirtualMachineTemplate{}).
		Named(templateapi.SingularResourceName).
		Complete(r)
}
