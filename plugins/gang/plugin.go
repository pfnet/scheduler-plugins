package gang

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/pfnet/scheduler-plugins/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

var (
	_ framework.Plugin            = &Plugin{}
	_ framework.EnqueueExtensions = &Plugin{}
	_ framework.PreFilterPlugin   = &Plugin{}
	_ framework.PermitPlugin      = &Plugin{}
	_ framework.ReservePlugin     = &Plugin{}
)

type Plugin struct {
	// Fields fixed in constructor
	config    PluginConfig
	fwkHandle framework.Handle

	// Fields that change at runtime
	gangs *Gangs // Gangs has own lock
}

func NewPlugin(_ context.Context, configuration runtime.Object, fwkHandle framework.Handle) (framework.Plugin, error) {
	registerMetrics.Do(func() {
		schedulermetrics.RegisterMetrics(gangSchedulingEventCounter)
	})

	// Load plugin config
	config, err := DecodePluginConfig(configuration)
	if err != nil {
		return nil, err
	}
	klog.Infof("%s: PluginConfig=%+v", PluginName, config)

	gangs := NewGangs(fwkHandle, fwkHandle.ClientSet(), config.TimeoutConfig(), config.GangAnnotationPrefix)
	plugin := &Plugin{
		config:    *config,
		fwkHandle: fwkHandle,
		gangs:     gangs,
	}

	// Cache gang Pods in plugin.gangs
	podInformer := fwkHandle.SharedInformerFactory().Core().V1().Pods().Informer()
	if _, err := podInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: plugin.filterPodEvent,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    plugin.handlePodAdd,
			UpdateFunc: plugin.handlePodUpdate,
			DeleteFunc: plugin.handlePodDelete,
		},
	}); err != nil {
		return nil, fmt.Errorf("add EventHandler on podInformer: %w", err)
	}

	if config.HealthCheckAddr != "" {
		go func() {
			healthCheck := newGangHealthServer(gangs)

			http.Handle("/gang-healthz", healthCheck)
			if err := http.ListenAndServe(config.HealthCheckAddr, nil); err != nil {
				klog.Fatalf("failed to start health check server: %v", err)
			}
		}()
	}

	return plugin, nil
}

func (p *Plugin) Name() string {
	return PluginName
}

// Note on parallelism:
// - Scheduler plugin entry point (PreFilter, Permit, Reserve, Unreserve)
//   - Only one of PreFilter, Permit, Reserve is called at a time
//   - For a specific pod, only one plugin entry point is called at a time
//   - For different pods, Unreserve can be called in parallel with other plugin entry points
// - Event handler (handlePodAdd, handlePodUpdate, handlePodDelete)
//   - Only one Pod event handler is called at a time
// - A scheduler plugin entry point and a Pod event handler can be called in parallel

// Scheduluer plugins entry points

func (p *Plugin) PreFilter(
	ctx context.Context, state *framework.CycleState, pod *corev1.Pod,
) (*framework.PreFilterResult, *framework.Status) {
	klog.V(5).Infof("%s: PreFilter start for pod %s/%s", p.Name(), pod.Namespace, pod.Name)

	var status *framework.Status
	defer func() {
		klog.V(5).Infof("%s: PreFilter end for pod %s/%s (status: %v)", p.Name(), pod.Namespace, pod.Name, status)
	}()

	if !IsGang(pod, p.config.GangAnnotationPrefix) {
		status, _ = allow("")
		return nil, status
	}

	status = p.gangs.PreFilter(ctx, state, pod)
	return nil, status
}

func (p *Plugin) PreFilterExtensions() framework.PreFilterExtensions { return nil }

func (p *Plugin) EventsToRegister(ctx context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{
		{
			Event: framework.ClusterEvent{
				Resource:   framework.Node,
				ActionType: framework.All,
			},
		},
		{
			Event: framework.ClusterEvent{
				// Note: framework.Pod doesn't work expectedly for non-scheduled Pod event.
				// We're doing the workaround in handlePodAdd.
				// see: https://github.com/kubernetes/kubernetes/issues/110175
				Resource:   framework.Pod,
				ActionType: framework.All,
			},
		},
	}, nil
}

func (p *Plugin) Permit(
	ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string,
) (*framework.Status, time.Duration) {
	var status *framework.Status
	var timeout time.Duration

	klog.V(5).Infof("%s: Permit start for pod %s/%s (node=%s)", p.Name(), pod.Namespace, pod.Name, nodeName)
	defer func() {
		klog.V(5).Infof("%s: Permit end for pod %s/%s (node=%s status=%v timeout=%s)",
			p.Name(), pod.Namespace, pod.Name, nodeName, status, timeout)
	}()

	status, timeout = p.gangs.Permit(state, pod)
	return status, timeout
}

func (p *Plugin) PostFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	p.gangs.PostFilter(ctx, pod)

	return nil, framework.NewStatus(framework.Unschedulable)
}

func (p *Plugin) PreEnqueue(ctx context.Context, pod *corev1.Pod) *framework.Status {
	if !IsGang(pod, p.config.GangAnnotationPrefix) {
		return nil
	}

	return p.gangs.PreEnqueue(pod)
}

func (p *Plugin) Reserve(ctx context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	return nil
}

// Unreserve is called when a waiting gang pod is rejected due to time out, and this rejects all waiting pods in the gang
func (p *Plugin) Unreserve(ctx context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) {
	klog.V(5).Infof("%s: Unreserve start for pod %s/%s (node=%s)", p.Name(), pod.Namespace, pod.Name, nodeName)
	defer func() {
		klog.V(5).Infof("%s: Unreserve end for pod %s/%s (node=%s)", p.Name(), pod.Namespace, pod.Name, nodeName)
	}()

	if !IsGang(pod, p.config.GangAnnotationPrefix) {
		klog.V(5).Infof("%s: Pod %s/%s is not a gang. Unreserve is noop.", p.Name(), pod.Namespace, pod.Name)
		return
	}

	p.gangs.Unreserve(pod, p.fwkHandle.EventRecorder())
}

// Pod event handlers

func (p *Plugin) filterPodEvent(obj interface{}) bool {
	var pod *corev1.Pod
	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		if po, ok := t.Obj.(*corev1.Pod); ok {
			pod = po
		} else {
			utilruntime.HandleError(fmt.Errorf("%s: unable to convert object %T to *v1.Pod", p.Name(), t.Obj))
		}
	default:
		utilruntime.HandleError(fmt.Errorf("%s: unable to handle object %T", p.Name(), obj))
	}

	return pod != nil &&
		utils.ResponsibleForPod(pod, p.config.SchedulerName) &&
		IsGang(pod, p.config.GangAnnotationPrefix) &&
		utils.IsNonCompletedPod(pod)
}

func (p *Plugin) handlePodAdd(obj interface{}) {
	// type assertion should never fail because handlers are registered with podInformer
	pod := obj.(*corev1.Pod)

	// ok must be true because non-gang Pods are filtered out in handler registration
	gangName, _ := GangNameOf(pod, p.config.GangAnnotationPrefix)

	klog.V(5).Infof("%s: handlePodAdd: pod=%s/%s gang=%s", p.Name(), pod.Namespace, pod.Name, gangName)
	p.gangs.AddOrUpdate(pod, p.fwkHandle.EventRecorder())
}

func (p *Plugin) handlePodUpdate(oldObj, newObj interface{}) {
	newPod, oldPod := newObj.(*corev1.Pod), oldObj.(*corev1.Pod)

	// TOOD(utam0k): Introduce QHint to make it wiser.
	// If pod.status or pod.spec isn't updated, there is no point in scheduling.
	if reflect.DeepEqual(oldPod.Status, newPod.Status) && reflect.DeepEqual(oldPod.Spec, newPod.Spec) && reflect.DeepEqual(oldPod.DeletionTimestamp, newPod.DeletionTimestamp) {
		return
	}

	gangName, _ := GangNameOf(newPod, p.config.GangAnnotationPrefix)
	klog.V(5).Infof("%s: handlePodUpdate: pod=%s/%s gang=%s", p.Name(), newPod.Namespace, newPod.Name, gangName)
	p.gangs.AddOrUpdate(newPod, p.fwkHandle.EventRecorder())
}

func (p *Plugin) handlePodDelete(obj interface{}) {
	pod := obj.(*corev1.Pod)
	gangName, _ := GangNameOf(pod, p.config.GangAnnotationPrefix)
	klog.V(5).Infof("%s: handlePodDelete: pod=%s/%s gang=%s", p.Name(), pod.Namespace, pod.Name, gangName)
	p.gangs.Delete(pod)
}

// Permit plugin responses

func allow(msg string) (*framework.Status, time.Duration) {
	return framework.NewStatus(framework.Success, msg), 0
}

func reject(msg string) (*framework.Status, time.Duration) {
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, msg), 0
}

func wait(msg string, duration time.Duration) (*framework.Status, time.Duration) {
	return framework.NewStatus(framework.Wait, msg), duration
}

// Scheduling event messages

func msgInternalError(format string, args ...interface{}) string {
	return PluginName + ": SchedulerInternalError: " + fmt.Sprintf(format, args...)
}
