package gang

import (
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/pfnet/scheduler-plugins/utils"
)

type SchedulingGang interface {
	Gang

	PreFilter(pod *corev1.Pod, timeoutConfig ScheduleTimeoutConfig) *framework.Status
	Permit(state *framework.CycleState, pod *corev1.Pod, timeoutConfig ScheduleTimeoutConfig) (*framework.Status, time.Duration)

	// Refresh rejects all waiting Pods and marks this SchedulingGang as done if it no longer
	// satisfies the invariant for gang scheduling.
	Refresh(ScheduleTimeoutConfig)

	// Timeout rejects all waiting Pods and mark this gang as done.
	// This is called by Unreserve plugin on timeout for a waiting gang Pod.
	Timeout()

	IsDone() bool

	// NonSchedulingGang returns the underlying Gang.
	NonSchedulingGang() Gang

	RejectWaitingPods(completionStatus GangSchedulingEvent, msgF msgForPodFunc)
}

// NewSchedulingGang creates a new SchedulingGang.
// SchedulingGang methods are thread-safe.
func NewSchedulingGang(gang Gang, fwkHandle framework.Handle, timeout time.Duration, gangAnnotationPrefix string) SchedulingGang {
	return &schedulingGangImpl{
		Gang:                 gang,
		gangAnnotationPrefix: gangAnnotationPrefix,
		fwkHandle:            fwkHandle,
		timeout:              timeout,
		done:                 make(chan struct{}),
	}
}

// Note on parallelism:
// - Only one of PreFilter, Permit, and Refresh is called at a time
//   - It is guaranteed by Gangs
// - Timeout can be called in parallel with PreFilter, Permit, and Refresh

type schedulingGangImpl struct {
	Gang
	fwkHandle            framework.Handle
	timeout              time.Duration
	gangAnnotationPrefix string

	sync.Mutex       // Protects accesses to completionStatus
	completionStatus GangSchedulingEvent
	done             chan struct{}
}

func (g *schedulingGangImpl) PreFilter(pod *corev1.Pod, timeoutConfig ScheduleTimeoutConfig) *framework.Status {
	gangNameToFilter, _ := GangNameOf(pod, g.gangAnnotationPrefix)
	// isGang is checked in Plugin.PreFilter

	klog.V(3).Infof("%s: PreFiltering pod %s/%s (gang %s) for scheduling gang %s",
		PluginName, pod.Namespace, pod.Name, gangNameToFilter, g.NameAndSpec().Name)

	if gangNameToFilter != g.NameAndSpec().Name {
		msg := msgInternalError(
			"SchedulingGang.PreFilter: Different gang %s incoming for scheduling gang %s",
			gangNameToFilter, g.NameAndSpec().Name,
		)
		klog.V(3).Info(msg)
		status, _ := reject(msg)
		return status
	}

	g.Lock()
	defer g.Unlock()

	if status, _ := g.rejectIfDone(pod); !status.IsSuccess() {
		return status
	}
	if status, _ := g.rejectAllAndDoneIfInvalid(pod, timeoutConfig, g.gangAnnotationPrefix); !status.IsSuccess() {
		return status
	}

	return framework.NewStatus(framework.Success, "")
}

func (g *schedulingGangImpl) Permit(state *framework.CycleState, pod *corev1.Pod, timeoutConfig ScheduleTimeoutConfig) (*framework.Status, time.Duration) {
	gangNameToPermit, _ := GangNameOf(pod, g.gangAnnotationPrefix)
	// isGang is checked in Plugin.Permit

	klog.V(3).Infof("%s: Permitting pod %s/%s (gang %s) for scheduling gang %s",
		PluginName, pod.Namespace, pod.Name, gangNameToPermit.Name, g.NameAndSpec().Name)

	if gangNameToPermit != g.NameAndSpec().Name {
		msg := msgInternalError(
			"SchedulingGang.Permit: Different gang %s incoming for scheduling gang %s",
			gangNameToPermit, g.NameAndSpec().Name,
		)
		klog.V(3).Info(msg)
		return reject(msg)
	}

	g.Lock()
	defer g.Unlock()

	if status, dur := g.rejectIfDone(pod); !status.IsSuccess() {
		return status, dur
	}
	if status, dur := g.rejectAllAndDoneIfInvalid(pod, timeoutConfig, g.gangAnnotationPrefix); !status.IsSuccess() {
		return status, dur
	}
	if status, dur := g.rejectAllAndDoneIfFullyScheduled(pod); !status.IsSuccess() {
		return status, dur
	}

	if status, dur := g.allowAllAndDoneIfFillingRunningGang(pod); status.IsSuccess() {
		return status, dur
	}
	if status, dur := g.allowAllAndDoneIfReady(pod); status.IsSuccess() {
		return status, dur
	}

	return g.waitForReady(state, pod, g.timeout)
}

func (g *schedulingGangImpl) Refresh(timeoutConfig ScheduleTimeoutConfig) {
	g.Lock()
	defer g.Unlock()

	if g.IsDone() {
		return
	}

	if ok, completionStatus := g.SatisfiesInvariantForScheduling(timeoutConfig); !ok {
		g.setDone(completionStatus)
		g.RejectWaitingPods(completionStatus, g.EventMessageForPodFunc(completionStatus))
	}
}

func (g *schedulingGangImpl) Timeout() {
	g.Lock()
	defer g.Unlock()

	if g.IsDone() {
		return
	}

	g.setDone(GangSchedulingTimedOut)
	g.RejectWaitingPods(GangSchedulingTimedOut, g.EventMessageForPodFunc(GangSchedulingTimedOut))
}

func (g *schedulingGangImpl) IsDone() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *schedulingGangImpl) NonSchedulingGang() Gang {
	return g.Gang
}

func (g *schedulingGangImpl) EventMessage(event GangSchedulingEvent, pod *corev1.Pod) string {
	ev := event
	if ev == "" {
		ev = "SchedulerInternalError"
	}

	return fmt.Sprintf(
		"%s: %s for pod %s/%s (gang=%s size=%d timeout=%s)",
		PluginName, ev, pod.Namespace, pod.Name, g.NameAndSpec().Name, g.NameAndSpec().Spec.Size, g.timeout,
	)
}

func (g *schedulingGangImpl) EventMessageForPodFunc(event GangSchedulingEvent) func(*corev1.Pod) string {
	return func(pod *corev1.Pod) string {
		return g.EventMessage(event, pod)
	}
}

// Methods below are non thread-safe
// g.Lock() is required.

func (g *schedulingGangImpl) rejectIfDone(pod *corev1.Pod) (*framework.Status, time.Duration) {
	if g.IsDone() {
		logFunc := klog.Errorf
		if g.completionStatus == GangSchedulingTimedOut {
			logFunc = klog.V(3).Infof
		}
		msg := g.EventMessage(g.completionStatus, pod)
		logFunc(msg)

		return reject(msg)
	}

	return nil, 0
}

func (g *schedulingGangImpl) rejectAllAndDoneIfInvalid(pod *corev1.Pod, timeoutConfig ScheduleTimeoutConfig, gangAnnotationPrefix string) (*framework.Status, time.Duration) {
	nameSpec := g.NameAndSpec()

	if ok, status := g.SatisfiesInvariantForScheduling(timeoutConfig); !ok {
		return g.rejectAllAndDone(pod, status, g.EventMessageForPodFunc(status))
	}

	gangToCheck, _ := GangNameAndSpecOf(pod, timeoutConfig, gangAnnotationPrefix)
	if nameSpec.Spec != gangToCheck.Spec ||
		!g.IsAllNonCompletedSpecIdenticalTo(gangToCheck.Spec, timeoutConfig) {
		return g.rejectAllAndDone(pod, GangSpecInvalid, g.EventMessageForPodFunc(GangSpecInvalid))
	}

	return nil, 0
}

func (g *schedulingGangImpl) rejectAllAndDoneIfFullyScheduled(pod *corev1.Pod) (*framework.Status, time.Duration) {
	nameSpec := g.NameAndSpec()

	numScheduled := g.CountPodIf(utils.IsAssignedAndNonCompletedPod)
	if numScheduled >= nameSpec.Spec.Size {
		return g.rejectAllAndDone(pod, GangFullyScheduled, g.EventMessageForPodFunc(GangFullyScheduled))
	}

	return nil, 0
}

func (g *schedulingGangImpl) allowAllAndDoneIfFillingRunningGang(
	pod *corev1.Pod,
) (*framework.Status, time.Duration) {
	nameSpec := g.NameAndSpec()

	numScheduled := g.CountPodIf(utils.IsAssignedAndNonCompletedPod)
	if numScheduled > 0 && numScheduled < nameSpec.Spec.Size {
		return g.allowAllAndDone(pod, FillingRunningGang, g.EventMessageForPodFunc(FillingRunningGang))
	}

	return reject("")
}

func (g *schedulingGangImpl) allowAllAndDoneIfReady(
	pod *corev1.Pod,
) (*framework.Status, time.Duration) {
	nameSpec := g.NameAndSpec()

	numWaiting := 0
	g.fwkHandle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if g.contains(wp.GetPod()) {
			numWaiting += 1
		}
	})

	if numWaiting+1 == nameSpec.Spec.Size {
		return g.allowAllAndDone(pod, GangReady, g.EventMessageForPodFunc(GangReady))
	}

	return reject("")
}

func (g *schedulingGangImpl) waitForReady(state *framework.CycleState, pod *corev1.Pod, timeout time.Duration) (*framework.Status, time.Duration) {
	msg := g.EventMessage(GangWaitForReady, pod)
	g.fwkHandle.EventRecorder().Eventf(pod, nil, corev1.EventTypeNormal, string(GangWaitForReady), "Scheduling", msg)
	klog.V(3).Info(msg)

	g.incEventCounter(GangWaitForReady)

	return wait(msg, timeout)
}

// getNamespacedName returns the namespaced name of the given Pod.
func getNamespacedName(p *corev1.Pod) string {
	return fmt.Sprintf("%v/%v", p.GetNamespace(), p.GetName())
}

type msgForPodFunc func(opd *corev1.Pod) string

func (g *schedulingGangImpl) rejectAllAndDone(
	pod *corev1.Pod, completionStatus GangSchedulingEvent, msgF msgForPodFunc,
) (*framework.Status, time.Duration) {
	g.setDone(completionStatus)
	g.RejectWaitingPods(completionStatus, msgF)

	msg := msgF(pod)
	klog.V(3).Info(msg)
	return reject(msg)
}

func (g *schedulingGangImpl) allowAllAndDone(
	pod *corev1.Pod, completionStatus GangSchedulingEvent, msgF msgForPodFunc,
) (*framework.Status, time.Duration) {
	g.setDone(completionStatus)

	// Allow waiting Pods
	g.fwkHandle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		pod := wp.GetPod()
		if g.contains(pod) {
			msg := msgF(pod)
			g.fwkHandle.EventRecorder().Eventf(
				pod, nil, corev1.EventTypeNormal, string(completionStatus), "Scheduling", msg)
			klog.V(3).Info(msg)
			wp.Allow(PluginName)
		}
	})

	// Allow incoming Pod
	msg := msgF(pod)
	g.fwkHandle.EventRecorder().Eventf(pod, nil, corev1.EventTypeNormal, string(completionStatus), "Scheduling", msg)
	klog.V(3).Info(msg)
	return allow(msg)
}

func (g *schedulingGangImpl) RejectWaitingPods(completionStatus GangSchedulingEvent, msgF msgForPodFunc) {
	g.fwkHandle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		pod := wp.GetPod()
		if g.contains(pod) {
			msg := msgF(pod)
			g.fwkHandle.EventRecorder().Eventf(
				pod, nil, corev1.EventTypeWarning, string(completionStatus), "Scheduling", msg)
			klog.V(3).Info(msg)
			wp.Reject(PluginName, msg)
		}
	})
}

func (g *schedulingGangImpl) setDone(status GangSchedulingEvent) {
	g.completionStatus = status
	close(g.done)

	nameSpec := g.NameAndSpec()
	klog.V(3).Infof(
		"%s: Scheduling for gang %s completed with status %s (size=%d timeout=%s)",
		PluginName, nameSpec.Name, status, nameSpec.Spec.Size, g.timeout,
	)

	g.incEventCounter(status)
}

func (g *schedulingGangImpl) contains(pod *corev1.Pod) bool {
	gangName, ok := GangNameOf(pod, g.gangAnnotationPrefix)
	return ok && gangName == g.NameAndSpec().Name
}

func (g *schedulingGangImpl) incEventCounter(event GangSchedulingEvent) {
	incEventCounter(event, g.NameAndSpec().Name.Namespace, g.NameAndSpec().Name.Name)
}
