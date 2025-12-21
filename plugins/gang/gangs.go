package gang

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pfnet/scheduler-plugins/utils"
	"github.com/sasha-s/go-deadlock"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const updatePositionAnnotationWorkerNum = 10

// TODO: More distinguishable name
type Gangs struct {
	gangAnnotationPrefix string

	fwkHandle     framework.Handle
	client        kubernetes.Interface
	timeoutConfig ScheduleTimeoutConfig

	mapLock deadlock.RWMutex  // Protects accesses to gangs map
	gangs   map[GangName]Gang // Gang implementations have own lock

	activateGangsPoolLock deadlock.Mutex
	// activateGangsPool has Gangs to activate.
	// Gangs will activate all of them and every time it runs Permit.
	activateGangsPool sets.Set[GangName]

	// In EventToRegister, framework.Pod event isn't work for non-scheduled Pod event.
	// see: https://github.com/kubernetes/kubernetes/issues/110175
	// Here, we record Pods rejected by this plugin AND it may be schedulable by any event for Pods in the same gang.
	// It's just a workaround and we can remove this if #110175 is resolved.
	podUIDs     sets.Set[types.UID]
	podUIDsLock deadlock.Mutex

	positionAnnotationUpdateWorkerPool []chan positionAnnotationUpdateTask
}

func NewGangs(ctx context.Context, fwkHandle framework.Handle, client kubernetes.Interface, timeoutConfig ScheduleTimeoutConfig, gangAnnotationPrefix string) *Gangs {
	deadlock.Opts.DeadlockTimeout = 3 * time.Minute
	deadlock.Opts.DisableLockOrderDetection = true

	gangs := &Gangs{
		gangAnnotationPrefix:               gangAnnotationPrefix,
		fwkHandle:                          fwkHandle,
		client:                             client,
		timeoutConfig:                      timeoutConfig,
		gangs:                              map[GangName]Gang{},
		activateGangsPool:                  sets.New[GangName](),
		podUIDs:                            sets.New[types.UID](),
		positionAnnotationUpdateWorkerPool: make([]chan positionAnnotationUpdateTask, updatePositionAnnotationWorkerNum),
	}

	// Start position annotation update workers.
	for i := 0; i < updatePositionAnnotationWorkerNum; i++ {
		gangs.positionAnnotationUpdateWorkerPool[i] = make(chan positionAnnotationUpdateTask)
		go gangs.updatePositionAnnotation(ctx, gangs.positionAnnotationUpdateWorkerPool[i])
	}

	return gangs
}

// Note on parallelism:
// - Scheduler plugin entry point (PreFilter, Permit, , Unreserve)
//   - Only one of PreFilter, Permit is called at a time
//   - For a specific pod, only one plugin entry point is called at a time
//   - For different pods, Unreserve can be called in parallel with other plugin entry points
// - Event handler (AddOrUpdate, Delete)
//   - Only one Pod event handler is called at a time
// - A scheduler plugin entry point and a Pod event handler can be called in parallel

// gangFirstPod implements fwk.StateData
type gangFirstPod struct{}

func (g gangFirstPod) Clone() fwk.StateData {
	return gangFirstPod{}
}

// PreEnqueue accept gang pods to be enqueued only when gang's position is PodPositionSchedulingCycle or PodPositionActiveQ.
// Thus, the possible scenarios the gang pods can pass here are:
// - All gang pods are rejected by this gang plugin, and PodsToActivate for them gets issued by the Permit.
// - Preemption happened for a gang Pod in the past scheduling cycle, and that Pod is moved to activeQ right after it moved to the unschedulable Pod pool.
func (gangs *Gangs) PreEnqueue(pod *corev1.Pod) *fwk.Status {
	nameSpec, ok := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)
	if !ok {
		return nil
	}

	gangs.mapLock.Lock()
	gangMapUnlocked := false
	defer func() {
		if !gangMapUnlocked {
			gangs.mapLock.Unlock()
		}
	}()

	gang, ok := gangs.gangs[nameSpec.Name]
	if !ok {
		// Gang has not been registered in AddOrUpdate().
		// (PreEnqueue sometimes run before AddOrUpdate().)
		msg := fmt.Sprintf(
			"%s: Gangs.PreEnqueue: gang %s for Pod %s/%s has not been registered yet",
			PluginName, nameSpec.Name, pod.Namespace, pod.Name,
		)
		klog.V(3).Info(msg)

		gang = NewGang(nameSpec, gangs.gangAnnotationPrefix)
		gang.AddOrUpdate(pod)
		gangs.gangs[nameSpec.Name] = gang
	}

	position := gang.GetPosition(pod.UID)
	if (position == PodPositionSchedulingCycle || position == PodPositionActiveQ) && gang.ReadyToGetSchedule() {
		// This Pod looks ready to be enqueued.
		// Only these two cases, reaching here:
		// - PodPositionActiveQ: this Pod is enqueued via PodToActivate issued in Permit
		// - PodPositionSchedulingCycle: this Pod is through preemption and may get schedulable in the next scheduling cycle.

		// Check scheduling invariant before enqueueing.
		if ok, gangEvent := gang.SatisfiesInvariantForScheduling(gangs.timeoutConfig); !ok {
			// This gang is invalid for some reasons (e.g., the num of pods is less than the required number of Pods),
			// so reject this Pod here.
			// This Pod will reach PreEnqueue again when, for example, another Pod is added to the gang.
			//
			// Note that we don't need to handle other gang Pods here;
			// they'll reach PreEnqueue soon, and the position will be updated to PodPositionReadyToSchedule below.

			msg := gang.EventMessage(gangEvent, pod)
			klog.V(3).Info(msg)
			status, _ := reject(msg)
			for _, p := range gang.Pods() {
				p := p
				gangs.fwkHandle.EventRecorder().Eventf(p, nil, corev1.EventTypeNormal, string(gangEvent), "Scheduling", msg)
			}

			gangs.putPosition(gang, pod, PodPositionUnschedulablePodPool)

			// Pods rejected here can be schedulable when an event for some Pods in the same gang happens.
			// And, _ideally_, it's supposed to be handled by Pod events in EventsToRegister,
			// but Pod event doesn't work expectedly for non-scheduled Pod event.
			// see: https://github.com/kubernetes/kubernetes/issues/110175
			// So, we, here, register those Pods and handle them in handlePodAdd as a workaround.
			gangs.podUIDsLock.Lock()
			defer gangs.podUIDsLock.Unlock()
			gangs.podUIDs.Insert(pod.UID)

			return status
		}

		// This gang is ready to be enqueued.

		klog.V(3).Infof("position update in PreEnqueue: %v/%s -> %s", pod.Name, position, PodPositionActiveQ)
		gangs.putPosition(gang, pod, PodPositionActiveQ)

		return nil
	}

	// This pod is tried to be enqueueed, so mark it as ReadyToSchedule.
	klog.V(3).Infof("position update in PreEnqueue: %v/%s -> %s", pod.Name, position, PodPositionReadyToSchedule)
	gangs.putPosition(gang, pod, PodPositionReadyToSchedule)
	// The position change may make the gang ready to schedulable.
	if gang.ReadyToGetSchedule() {
		if time.Since(gang.LastActiviaionTime()) > 10*time.Minute {
			// We need to unlock gangs.mapLock before acquiring gangs.activateGangsPoolLock
			// to prevent deadlock.
			gangs.mapLock.Unlock()
			gangMapUnlocked = true

			gangs.activateGangsPoolLock.Lock()
			if !gangs.activateGangsPool.Has(gang.NameAndSpec().Name) {
				klog.Fatalf("gang %s may be in the infinite loop (Gang=%s) ", gang.NameAndSpec().Name, gangs.String())
			}
			gangs.activateGangsPoolLock.Unlock()
		}
		gangs.fwkHandle.EventRecorder().Eventf(pod, nil, corev1.EventTypeNormal, "WaitActivation", "Scheduling", "all Pods in the gang are now ready to schedule. Waiting for activation.")
		return fwk.NewStatus(fwk.Unschedulable, "This Pod will be soon activated along with other gang Pods")
	}

	gangs.fwkHandle.EventRecorder().Eventf(pod, nil, corev1.EventTypeNormal, "WaitOtherGangsToGetReadyToSchedule", "Scheduling", fmt.Sprintf("the other pod(s) in the same gang isn't ready to schedule: %v", gang.UnreadyToSchedulePodNames()))

	return fwk.NewStatus(fwk.Unschedulable, "the other pod(s) in the same gang isn't ready to schedule")
}

func (gangs *Gangs) PostFilter(ctx context.Context, pod *corev1.Pod) {
	nameSpec, ok := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)
	if !ok {
		return
	}

	gangs.mapLock.RLock()
	gang, ok := gangs.gangs[nameSpec.Name]
	gangs.mapLock.RUnlock()
	if !ok {
		// Gang has not registered in gangs.gangs by AddOrUpdate() or PreEnqueue. shouldn't happen.
		msg := fmt.Sprintf(
			"%s: Gangs.PreFilter: gang %s for Pod %s/%s has not been registered yet",
			PluginName, nameSpec.Name, pod.Namespace, pod.Name,
		)
		klog.Error(msg)
		return
	}

	position := gang.GetPosition(pod.UID)
	if position == PodPositionReadyToSchedule {
		// If it's PodPositionReadyToSchedule, that means the Pod got rejected by the gang plugin PreFilter because other Pods in the same gang got unschedulable.
		// In this case, we want to keep this Pod registered with this position.
		return
	}
	if position == PodPositionUnknown {
		klog.ErrorS(nil, "PostFilter: unknown position registered for the incoming Pod", "pod", klog.KObj(pod))
	}

	klog.V(3).Infof("position update in PostFilter: %v/%s -> %s", pod.Name, position, PodPositionUnschedulablePodPool)
	gangs.putPosition(gang, pod, PodPositionUnschedulablePodPool)
	gangs.cleanup(ctx, gang)
}

// cleanup reject all waiting Pods and un-nominate a Node.
func (gangs *Gangs) cleanup(ctx context.Context, gang Gang) {
	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()

	schedulingGang, isScheduling := gang.(SchedulingGang)
	if isScheduling {
		// all waiting pods in the same gang should be rejected.
		schedulingGang.RejectWaitingPods(GangOtherPodGetsRejected, schedulingGang.EventMessageForPodFunc(GangOtherPodGetsRejected))
	}

	gang.IterateOverPods(func(pod *corev1.Pod) {
		position := gang.GetPosition(pod.UID)
		if position == PodPositionWaitingOnPermit {
			// As we rejected all waiting Pods, we should change those Pods' position to PodPositionReadyToSchedule.
			klog.V(3).Infof("position update in cleanup: %v/%s -> %s", pod.Name, position, PodPositionReadyToSchedule)
			gangs.putPosition(gang, pod, PodPositionReadyToSchedule)
		}

		// If some Pods get NominatedNodeName by Preemption, we should un-nominate those Nodes
		// so that the Pod won't reserve the Node until next chance this gang gets scheduled.
		if pod.Status.NominatedNodeName != "" {
			// This Pod should be ReadyToSchedule
			// because the scheduler has already done the preemption for that Pod and thus the cluster should have the capacity for this Pod. (at least right after the preemption.)
			klog.V(3).Infof("position update in cleanup: %v/%s -> %s", pod.Name, position, PodPositionReadyToSchedule)
			gangs.putPosition(gang, pod, PodPositionReadyToSchedule)

			oldStatus := pod.Status.DeepCopy()
			podStatusCopy := pod.Status.DeepCopy()
			podStatusCopy.NominatedNodeName = ""
			if err := util.PatchPodStatus(ctx, gangs.client, pod.Name, pod.Namespace, oldStatus, podStatusCopy); err != nil {
				klog.ErrorS(err, "failed to remove NominatedNodeName on gang Pod", "pod", klog.KObj(pod))
				return
			}

			// TODO(utam0k): Uncomment this after investigating the reason why it will cause a deadlock.
			//
			// pinfo, err := framework.NewPodInfo(pod)
			// if err != nil {
			// 	klog.ErrorS(err, "failed to create PodInfo for gang Pod", "pod", klog.KObj(pod))
			// 	return
			// }
			// gangs.fwkHandle.AddNominatedPod(klog.FromContext(ctx), pinfo, &framework.NominatingInfo{NominatingMode: framework.ModeOverride, NominatedNodeName: ""})
		}
	})
}

func (gangs *Gangs) PreFilter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod) (status *fwk.Status) {
	nameSpec, isGang := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)
	if !isGang {
		return nil
	}

	gangs.mapLock.RLock()
	gang, ok := gangs.gangs[nameSpec.Name]
	gangs.mapLock.RUnlock()
	if !ok {
		// Gang has not been registered with gangs.gangs in AddOrUpdate() or PreEnqueue. shouldn't happen.
		msg := fmt.Sprintf(
			"%s: Gangs.PreFilter: gang %s for Pod %s/%s has not been registered yet. Rejecting.",
			PluginName, nameSpec.Name, pod.Namespace, pod.Name,
		)
		klog.V(3).Info(msg)
		status, _ := reject(msg)
		return status
	}

	if !gang.ReadyToGetSchedule() {
		position := gang.GetPosition(pod.UID)
		if position == PodPositionUnschedulablePodPool {
			// Here is the unusual path for the workaround of the bug:
			// https://github.com/kubernetes/kubernetes/issues/118226
			//
			// Since it rarely happens, put ReadyToSchedule on this Pod so that this bug won't block entire gang scheduling.
			klog.V(3).Infof("position update in PreFilter: %v/%s -> %s", pod.Name, position, PodPositionReadyToSchedule)
			gangs.putPosition(gang, pod, PodPositionReadyToSchedule)
			// In this path, we don't do gangs.cleanup because we can just retry to schedule this Pod with the appropriate position.
			return fwk.NewStatus(fwk.Unschedulable, "This pod isn't ready to schedule")
		}

		// If any of the gang Pods are marked unschedulable,
		// PreFilter rejects all other gang Pods.
		gangs.cleanup(ctx, gang)
		// This Pod is still ReadyToSchedule.
		klog.V(3).Infof("position update in PreFilter: %v/%s -> %s", pod.Name, position, PodPositionReadyToSchedule)
		gangs.putPosition(gang, pod, PodPositionReadyToSchedule)

		msg := gang.EventMessage(GangNotReadyToSchedule, pod)
		status, _ := reject(msg)
		return status
	}

	klog.V(3).Infof("position update in PreFilter: %v/%s", pod.Name, PodPositionSchedulingCycle)
	gangs.putPosition(gang, pod, PodPositionSchedulingCycle)

	// Reset scheduling gang to non-scheduling gang if it is done
	schedulingGang, isScheduling := gang.(SchedulingGang)
	if isScheduling && schedulingGang.IsDone() {
		gangs.mapLock.Lock()
		gangs.gangs[nameSpec.Name] = schedulingGang.NonSchedulingGang()
		gangs.mapLock.Unlock()

		schedulingGang = nil
		isScheduling = false
	}

	if !isScheduling {
		// New scheduling gang starts here
		timeout := nameSpec.Spec.TimeoutBase + time.Duration(rand.Intn(nameSpec.Spec.TimeoutJitterSeconds))*time.Second
		schedulingGang = NewSchedulingGang(gang, gangs.fwkHandle, timeout, gangs.gangAnnotationPrefix)
		gangs.mapLock.Lock()
		gangs.gangs[nameSpec.Name] = schedulingGang
		gangs.mapLock.Unlock()

		klog.Infof(
			"%s: Gang scheduling for gang %s (size=%d timeout=%s) started",
			PluginName, nameSpec.Name, nameSpec.Spec.Size, timeout,
		)

		// Let unique zone plugin know that this Pod is the first of a new scheduling gang
		state.Write(StateKeyGangFirstPod, gangFirstPod{})
	}

	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()
	return schedulingGang.PreFilter(pod, gangs.timeoutConfig)
}

// putPosition registers the Pod's position.
// Also, if the transition makes gang ready to get schedule,
// the gang is put into activateGangsPool.
func (gangs *Gangs) putPosition(gang Gang, pod *corev1.Pod, position PodPosition) {
	wasReadyToGetSchedule := gang.ReadyToGetSchedule() && // this gang has already been ready to get schedule
		gang.GetPosition(pod.UID) != PodPositionUnknown // AND, this Pod was already registered

	gang.PutPosition(pod, position)

	readyToGetSchedule := gang.ReadyToGetSchedule()

	// Push the position update task to the worker pool.
	go func() {
		task := positionAnnotationUpdateTask{pod: pod, position: position}
		workerIndex := hashPodUID(pod.UID) % uint32(len(gangs.positionAnnotationUpdateWorkerPool))
		gangs.positionAnnotationUpdateWorkerPool[workerIndex] <- task
	}()

	// The position change makes the gang ready to schedulable.
	if readyToGetSchedule && !wasReadyToGetSchedule {
		gangs.activateGangsPoolLock.Lock()
		gangs.activateGangsPool.Insert(gang.NameAndSpec().Name)
		gangs.activateGangsPoolLock.Unlock()
		klog.Infof("pod %s/%s position updated to %s and we put the gang %s to activateGangsPool", pod.Namespace, pod.Name, position, gang.NameAndSpec().Name)
	} else {
		klog.Infof("pod %s/%s position updated to %s though, we don't put the gang %s to activateGangsPool. (wasReadyToGetSchedule: %v, ReadyToGetSchedule: %v)", pod.Namespace, pod.Name, position, gang.NameAndSpec().Name, wasReadyToGetSchedule, readyToGetSchedule)
	}
}

// activatePods activates all Pods in activateGangsPool.
// It uses PodsToActivate feature that the scheduling framework provides,
// which moves all Pods that we register in the cycle state from the unschedulable Pod pool to the activeQ.
func (gangs *Gangs) activatePods(state fwk.CycleState) {
	// In addition to activateGangsPoolLock, we must lock gangs.mapLock during entire this process
	// because if a new Pod is registered in gang during this process,
	// we might miss to activate some gang Pods.
	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()

	gangs.activateGangsPoolLock.Lock()
	defer gangs.activateGangsPoolLock.Unlock()

	// Get all Pods from all gangs in activateGangsPool.
	podsToActivate := []*corev1.Pod{}
	for _, gangName := range gangs.activateGangsPool.UnsortedList() {
		gang, ok := gangs.gangs[gangName]
		if !ok {
			klog.Warning("A gang is not found in gangs.gangs despite it's in activateGangsPool", "gang", gangName)
			continue
		}

		for _, p := range gang.Pods() {
			gang.PutPosition(p, PodPositionActiveQ)
			gangs.fwkHandle.EventRecorder().Eventf(p, nil, corev1.EventTypeNormal, "Activated", "Scheduling", "Pod is activated and the scheduler will soon try to schedule them.")
			podsToActivate = append(podsToActivate, p)
		}
	}

	if len(podsToActivate) == 0 {
		return
	}

	c, err := state.Read(framework.PodsToActivateKey)
	if err != nil {
		// shouldn't be happened...
		klog.Errorf("Failed to read PodsToActivate by PodsToActivateKey from CycleState: %v", err)
		return
	}

	s, ok := c.(*framework.PodsToActivate)
	if !ok {
		// shouldn't be happened...
		klog.Errorf("Failed to convert into PodsToActivate: %v", err)
		return
	}

	s.Lock()
	activatedPodNames := make([]string, len(podsToActivate))
	for i, pod := range podsToActivate {
		activatedPodNames[i] = getNamespacedName(pod)
		s.Map[getNamespacedName(pod)] = pod
	}
	klog.V(3).InfoS("activate gang Pods", "pods(namespace/name)", activatedPodNames)
	s.Unlock()

	// reset activateGangsPool
	gangs.activateGangsPool = sets.New[GangName]()
}

func (gangs *Gangs) Permit(state fwk.CycleState, pod *corev1.Pod) (retStatus *fwk.Status, _ time.Duration) {
	defer func() {
		if retStatus.IsSuccess() || retStatus.IsWait() {
			// Issue PodsToActivate when Permit returns success or wait
			// because PodToActivate is 100% effective in these cases.
			gangs.activatePods(state)
		}
	}()

	nameSpec, ok := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)
	if !ok {
		return allow(fmt.Sprintf("Pod %s/%s is not a gang", pod.Namespace, pod.Name))
	}

	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()
	gang, ok := gangs.gangs[nameSpec.Name]
	if !ok {
		// Gang has not been registered with gangs.gangs in AddOrUpdate().
		msg := fmt.Sprintf(
			"%s: Gangs.Permit: gang %s for Pod %s/%s has not been registered yet. Rejecting.",
			PluginName, nameSpec.Name, pod.Namespace, pod.Name,
		)
		klog.V(3).Info(msg)
		return reject(msg)
	}

	schedulingGang, ok := gang.(SchedulingGang)
	if !ok {
		msg := msgInternalError("Gang.Permit: gang %s is not in scheduling. Rejecting.", nameSpec.Name)
		klog.Error(msg)
		return reject(msg)
	}

	status, t := schedulingGang.Permit(state, pod, gangs.timeoutConfig)
	if status.IsWait() {
		klog.V(3).Infof("position update: %v/%s in Permit", pod.Name, PodPositionWaitingOnPermit)
		gangs.putPosition(gang, pod, PodPositionWaitingOnPermit)
	} else if status.IsSuccess() {
		gang.IterateOverPods(func(pod *corev1.Pod) {
			klog.V(3).Infof("position update to %v/%s in Permit", pod.Name, PodPositionCompleted)
			gangs.putPosition(gang, pod, PodPositionCompleted)
		})
	}

	return status, t
}

func (gangs *Gangs) Unreserve(pod *corev1.Pod, recorder events.EventRecorder) {
	nameSpec, _ := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)

	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()

	gang, ok := gangs.gangs[nameSpec.Name]
	if !ok {
		msg := fmt.Sprintf(
			"%s: Gangs.Unreserve: gang %s for Pod %s/%s has not been registered yet. Rejecting.",
			PluginName, nameSpec.Name, pod.Namespace, pod.Name,
		)
		klog.V(3).Info(msg)
	}

	position := gang.GetPosition(pod.UID)
	if position != PodPositionWaitingOnPermit {
		// If Position isn't PodPositionWaitingOnPermit,
		// this pod isn't come here due to timeout, but come here by being rejected in PostFilter.
		return
	}

	schedulingGang, ok := gang.(SchedulingGang)
	if !ok {
		msg := msgInternalError("Gang.Unreserve: gang %s is not in scheduling. Rejecting.", nameSpec.Name)
		klog.Error(msg)
	}

	msg := schedulingGang.EventMessageForPodFunc(GangSchedulingTimedOut)(pod)
	// Timeout below does not record an event for the Pod handled here because it is no longer a waiting pod,
	// so we record an event for the pod here
	recorder.Eventf(pod, nil, corev1.EventTypeWarning, string(GangSchedulingTimedOut), "Scheduling", msg)
	klog.V(3).Info(msg)

	klog.V(3).Infof("position update: %v/%s -> %s in Unreserve", pod.Name, position, PodPositionReadyToSchedule)
	gangs.putPosition(gang, pod, PodPositionReadyToSchedule)
	schedulingGang.Timeout()
}

func (gangs *Gangs) AddOrUpdate(pod *corev1.Pod, recorder events.EventRecorder) {
	nameSpec, _ := GangNameAndSpecOf(pod, gangs.timeoutConfig, gangs.gangAnnotationPrefix)
	// isGang must be true because non-gang Pods are filtered out in handler registration

	klog.V(3).Infof("%s: Gangs.AddOrUpdated pod %s/%s to gang %s (Gangs=%s)",
		PluginName, pod.Namespace, pod.Name, nameSpec.Name, gangs.String())
	defer func() {
		klog.V(3).Infof("%s: Gangs.AddOrUpdate finish (Gangs=%s)", PluginName, gangs.String())
	}()

	gangs.mapLock.Lock()
	defer gangs.mapLock.Unlock()
	gang, ok := gangs.gangs[nameSpec.Name]

	numTerminatingBefore := 0
	if ok {
		numTerminatingBefore = gang.CountPodIf(utils.IsTerminatingPod)
		gang.AddOrUpdate(pod)

		gangs.podUIDsLock.Lock()
		defer gangs.podUIDsLock.Unlock()
		for _, p := range gang.Pods() {
			if gangs.podUIDs.Has(p.UID) {
				klog.V(3).Infof("position update: %v/%s in AddOrUpdate", pod.Name, PodPositionReadyToSchedule)
				gangs.putPosition(gang, p, PodPositionReadyToSchedule)
				gangs.podUIDs.Delete(p.UID)
			}
		}
	} else {
		gang = NewGang(nameSpec, gangs.gangAnnotationPrefix)
		gang.AddOrUpdate(pod)
		gangs.gangs[nameSpec.Name] = gang
	}

	numTerminatingAfter := gang.CountPodIf(utils.IsTerminatingPod)

	// If observed terminating pod for the first time, all the pods in the gang should be deleted.
	if !gang.IsDeleting() && numTerminatingBefore == 0 && numTerminatingAfter > 0 {
		klog.V(3).Infof(
			"%s: Gangs.AddOrUpdate detected terminating pods (numTerminatingBefore=%d After=%d)",
			PluginName, numTerminatingBefore, numTerminatingAfter)
		incEventCounter(DeletedAsPartOfGang, nameSpec.Name.Namespace, nameSpec.Name.Name)

		// Mark this gang as "being deleted now"
		gang.SetDeleting(true)

		// Wait until all deletion complete
		wg := sync.WaitGroup{}
		gang.IterateOverPods(func(pod *corev1.Pod) {
			wg.Add(1)

			go func(gn GangName) {
				defer wg.Done()

				// Mark DeletedAsPartOfGang event for all Pods in the gang
				recorder.Eventf(pod, nil,
					corev1.EventTypeNormal, string(DeletedAsPartOfGang), "Scheduling", fmt.Sprintf("gang: %s", gn))

				if utils.IsTerminatingPod(pod) {
					return
				}

				klog.V(3).Infof(
					"%s: %s (pod=%s/%s gang=%s)", PluginName, DeletedAsPartOfGang, pod.Namespace, pod.Name, gn)

				if err := gangs.client.CoreV1().Pods(pod.Namespace).Delete(
					context.Background(), pod.Name, metav1.DeleteOptions{},
				); err != nil {
					if apierrors.IsNotFound(err) {
						klog.Errorf("%s: %s: Pod %s/%s (gang=%s) is not found",
							PluginName, DeletedAsPartOfGang, pod.Namespace, pod.Name, gn)
					} else {
						klog.Errorf("%s: Error in %s: %v (pod=%s/%s gang=%s)",
							PluginName, DeletedAsPartOfGang, err, pod.Namespace, pod.Name, gn)
					}
				}
			}(nameSpec.Name)
		})

		// When all deletion complete, remove "being deleted now" mark
		go func() {
			wg.Wait()
			gang.SetDeleting(false)
		}()
	}

	if schedulingGang, ok := gang.(SchedulingGang); ok {
		schedulingGang.Refresh(gangs.timeoutConfig)
	}
}

func (gangs *Gangs) Delete(pod *corev1.Pod) {
	gangName, _ := GangNameOf(pod, gangs.gangAnnotationPrefix)
	// isGang must be true because non-gang Pods are filtered out in handler registration

	klog.V(3).Infof("%s: Gangs.Delete pod %s/%s from gang %s (Gangs=%s)",
		PluginName, pod.Namespace, pod.Name, gangName, gangs.String())
	defer func() {
		klog.V(3).Infof("%s: Gangs.Delete finish (Gangs=%s)", PluginName, gangs.String())
	}()

	gangs.mapLock.Lock()
	defer gangs.mapLock.Unlock()

	if gang, ok := gangs.gangs[gangName]; ok {
		gang.Delete(pod)
		if gang.CountPod() == 0 {
			delete(gangs.gangs, gangName)
		}

		if schedulingGang, ok := gang.(SchedulingGang); ok {
			schedulingGang.Refresh(gangs.timeoutConfig)
		}
	}
}

// String implements fmt.Stringer.
// Returns a human-readable string representation of the gangs.
//
// Does not acquire the lock.
func (gangs *Gangs) String() string {
	gangs.mapLock.RLock()
	defer gangs.mapLock.RUnlock()

	var b strings.Builder
	b.WriteString("{")

	i := 0
	for _, gang := range gangs.gangs {
		b.WriteString(gang.String())
		if i < len(gangs.gangs)-1 {
			b.WriteString(", ")
		}
		i++
	}
	b.WriteString("}")

	return b.String()
}
