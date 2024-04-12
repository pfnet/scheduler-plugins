package gang

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/pfnet/scheduler-plugins/utils"
)

type Gang interface {
	fmt.Stringer

	// NameAndSpec returns the GangNameAndSpec of this Gang.
	NameAndSpec() *GangNameAndSpec

	// AddOrUpdate adds a given Pod to this Gang, or updates a Pod in this Gang.
	AddOrUpdate(*corev1.Pod)
	// Delete deletes a given Pod from this Gang. If the Pod is not in this Gang, Delete is a no-op.
	Delete(*corev1.Pod)

	// Pods returns all Pods in the gang.
	Pods() []*corev1.Pod
	// CountPod returns the number of Pods in this Gang.
	CountPod() int
	// CountPodIf returns the number of Pods that meets a given predicate in this Gang.
	CountPodIf(predicate func(*corev1.Pod) bool) int

	// SatisfiesInvariantForgScheduling checks that this Gang satisfy an invariant for gang
	// scheduling. If this Gang does not sastisfy the invariant, this plugin does not start
	// scheduling of this Gang and rejects it immediately.
	SatisfiesInvariantForScheduling(ScheduleTimeoutConfig) (bool, GangSchedulingEvent)

	// IterateOverPods iterates over the Pods in this Gang while applying a given function.
	// The function can mutate the Pods.
	IterateOverPods(func(pod *corev1.Pod))

	// IsAllNonCompleletedSpecIdenticalTo checks that the gang specs of all non-completed Pods in
	// this Gang are identical to a given gang spec.
	IsAllNonCompletedSpecIdenticalTo(GangSpec, ScheduleTimeoutConfig) bool

	// Mark this Gang as "being deleted now".
	SetDeleting(deleting bool)
	// Returns whether this Gang is now being deleted.
	IsDeleting() bool

	// EventMessage returns a message to be notified as a plugin response or Kubernetes event.
	EventMessage(event GangSchedulingEvent, pod *corev1.Pod) string
	EventMessageForPodFunc(event GangSchedulingEvent) func(*corev1.Pod) string

	GetPosition(podUID types.UID) PodPosition
	PutPosition(pod *corev1.Pod, position PodPosition)
	ReadyToGetSchedule() bool
	UnreadyToSchedulePodNames() []string
}

// NewGang creates a new Gang.
// Gang interface methods are *not* thread-safe.
// But as far as accessed from Gangs, Gang methods are called sequentially because Gangs acquires a
// lock when accessing its gangs map.
func NewGang(nameSpec GangNameAndSpec, gangAnnotationPrefix string) Gang {
	return &gangImpl{
		gangAnnotationPrefix:  gangAnnotationPrefix,
		GangNameAndSpec:       nameSpec,
		pods:                  map[types.UID]*corev1.Pod{},
		deleting:              false,
		positions:             map[types.UID]PodPosition{},
		unreadyToSchedulePods: sets.New[string](),
	}
}

// PodPosition represents the place where a pod can be.
type PodPosition int

const (
	// PodPositionUnknown represents that we don't know where a Pod is in the scheduler.
	//
	// The gang PreEnqueue is responsible to register a position for a newly created Pod.
	// Until that, a newly created Pod's position will be Unknown.
	PodPositionUnknown PodPosition = iota
	// PodPositionUnschedulablePodPool represents that a Pod is or should be in the Unschedulable Pod Pool.
	//
	// The gang PostFilter is responsible to change a position for a rejected Pod to PodPositionUnschedulablePodPool.
	PodPositionUnschedulablePodPool
	// PodPositionReadyToSchedule represents that a Pod is in the Unschedulable Pod Pool, but ready to get schedule AND PodsToActivate isn't issued for it yet.
	// When all Pods in the gang get PodPositionReadyToSchedule, then PodsToActivate is expected to get issued soon from the gang Permit.
	//
	// There are multiple scenario that a Pod can get PodPositionReadyToSchedule,
	// the most popular one among them is PreEnqueue changing the given Pod's position to PodPositionReadyToSchedule.
	// The scheduler tries to move a Pod from unschedulable Pod Pool to activeQ when an event which may make a Pod schedulable happens.
	// So, we can regard a Pod which is coming on PreEnqueue as a ready-to-schedule Pod.
	PodPositionReadyToSchedule
	// PodPositionActiveQ represents that a Pod is in ActiveQ. Or in the Unschedulable Pod Pool but PodsToActivate has been issued for the Pod.
	//
	// The gang Permit is responsible to change a position to PodPositionActiveQ when it issues PodsToActivate for the Pod.
	// Those Pods will soon reach the gang PreEnqueue, and get accepted to enqueued to activeQ.
	PodPositionActiveQ
	// PodPositionSchedulingCycle represents that a Pod is under scheduling.
	//
	// The gang PreFilter is responsible to change a position to PodPositionSchedulingCycle when it accepts the Pod.
	PodPositionSchedulingCycle
	// PodPositionWaitingOnPermit represents that a Pod is waiting on permit.
	//
	// The gang Permit is responsible to change a position to PodPositionWaitingOnPermit when the Pod go through Permit plugin with Wait status.
	PodPositionWaitingOnPermit
)

func (p PodPosition) String() string {
	switch p {
	case PodPositionUnknown:
		return "Unknown"
	case PodPositionUnschedulablePodPool:
		return "UnschedulablePodPool"
	case PodPositionReadyToSchedule:
		return "ReadyToSchedule"
	case PodPositionActiveQ:
		return "ActiveQ"
	case PodPositionSchedulingCycle:
		return "SchedulingCycle"
	case PodPositionWaitingOnPermit:
		return "WaitingOnPermit"
	default:
		return "nil"
	}
}

type gangImpl struct {
	GangNameAndSpec

	gangAnnotationPrefix string

	// podsLock protects accesses to `pods` maps.
	podsLock sync.RWMutex
	pods     map[types.UID]*corev1.Pod

	deleting bool

	// positionsLock protects accesses to `positions` maps and `unreadyToSchedulePods`.
	positionsLock sync.RWMutex
	// positions where each unscheduled Pods are expected to be.
	positions map[types.UID]PodPosition
	// unreadyToSchedulePods aren't ready to get schedule.
	unreadyToSchedulePods sets.Set[string]
}

// GetPosition gets PodPosition from store and returns it.
func (g *gangImpl) GetPosition(podUID types.UID) PodPosition {
	g.positionsLock.RLock()
	defer g.positionsLock.RUnlock()

	po, ok := g.positions[podUID]
	if ok {
		return po
	}
	return PodPositionUnknown
}

func (g *gangImpl) ReadyToGetSchedule() bool {
	return g.unreadyToSchedulePods.Len() == 0
}

func (g *gangImpl) UnreadyToSchedulePodNames() []string {
	g.positionsLock.RLock()
	defer g.positionsLock.RUnlock()

	names := make([]string, 0, len(g.unreadyToSchedulePods))
	for p := range g.unreadyToSchedulePods {
		names = append(names, p)
	}
	return names
}

// PutCondition puts PodPosition to store.
// If the data for Pod already exist, it'll be overwrited.
func (g *gangImpl) PutPosition(pod *corev1.Pod, position PodPosition) {
	g.positionsLock.Lock()
	defer g.positionsLock.Unlock()

	g.positions[pod.UID] = position
	if position == PodPositionUnknown || position == PodPositionUnschedulablePodPool {
		g.unreadyToSchedulePods.Insert(getNamespacedName(pod))
	} else {
		g.unreadyToSchedulePods.Delete(getNamespacedName(pod))
	}
}

// String implements fmt.Stringer interface.
// Returns a human-readable string representation of the gang.
func (g *gangImpl) String() string {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	pods := make([]string, 0, len(g.pods))
	for _, p := range g.pods {
		pods = append(pods, fmt.Sprintf("%s(%s)", p.Name, p.Status.Phase))
	}
	return g.Name.String() + ": [" + strings.Join(pods, ", ") + "]"
}

func (g *gangImpl) NameAndSpec() *GangNameAndSpec {
	return &g.GangNameAndSpec
}

func (g *gangImpl) AddOrUpdate(pod *corev1.Pod) {
	g.podsLock.Lock()
	defer g.podsLock.Unlock()

	g.pods[pod.UID] = pod
}

func (g *gangImpl) Delete(pod *corev1.Pod) {
	g.podsLock.Lock()
	defer g.podsLock.Unlock()

	delete(g.pods, pod.UID)
}

func (g *gangImpl) CountPod() int {
	return len(g.pods)
}

// Pods returns all Pods in the gang.
func (g *gangImpl) Pods() []*corev1.Pod {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	pods := make([]*corev1.Pod, 0, len(g.pods))
	for _, p := range g.pods {
		pods = append(pods, p.DeepCopy())
	}
	return pods
}

func (g *gangImpl) CountPodIf(pred func(pod *corev1.Pod) bool) int {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	c := 0
	for _, pod := range g.pods {
		if pred(pod) {
			c++
		}
	}
	return c
}

func (g *gangImpl) SatisfiesInvariantForScheduling(
	timeoutConfig ScheduleTimeoutConfig,
) (bool, GangSchedulingEvent) {
	if !g.isAllNonCompletedSpecIdentical(timeoutConfig) {
		return false, GangSpecInvalid
	}

	if g.CountPodIf(utils.IsNonCompletedPod) < g.Spec.Size {
		return false, GangNotReady
	}

	if g.CountPodIf(utils.IsTerminatingPod) > 0 {
		return false, GangWaitForTerminating
	}

	if g.CountPodIf(utils.IsAssignedAndNonCompletedPod) >= g.Spec.Size {
		return false, GangFullyScheduled
	}

	return true, ""
}

func (g *gangImpl) IterateOverPods(f func(pod *corev1.Pod)) {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	for _, pod := range g.pods {
		f(pod)
	}
}

func (g *gangImpl) IsAllNonCompletedSpecIdenticalTo(
	target GangSpec, timeoutConfig ScheduleTimeoutConfig,
) bool {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	for _, pod := range g.pods {
		if utils.IsNonCompletedPod(pod) {
			s := gangSpecOf(pod, timeoutConfig, g.gangAnnotationPrefix)
			if s != target {
				return false
			}
		}
	}
	return true
}

func (g *gangImpl) SetDeleting(deleting bool) {
	g.deleting = deleting
}

func (g *gangImpl) IsDeleting() bool {
	return g.deleting
}

func (g *gangImpl) EventMessage(event GangSchedulingEvent, pod *corev1.Pod) string {
	ev := event
	if ev == "" {
		ev = "SchedulerInternalError"
	}

	jitter := time.Duration(g.Spec.TimeoutJitterSeconds) * time.Second
	return fmt.Sprintf(
		"%s: %s for pod %s/%s (gang=%s size=%d timeout=%s+[0,%s))",
		PluginName, ev, pod.Namespace, pod.Name, g.Name, g.Spec.Size, g.Spec.TimeoutBase, jitter,
	)
}

func (g *gangImpl) EventMessageForPodFunc(event GangSchedulingEvent) func(*corev1.Pod) string {
	return func(pod *corev1.Pod) string {
		return g.EventMessage(event, pod)
	}
}

func (g *gangImpl) isAllNonCompletedSpecIdentical(timeoutConfig ScheduleTimeoutConfig) bool {
	g.podsLock.RLock()
	defer g.podsLock.RUnlock()

	var spec *GangSpec
	for _, pod := range g.pods {
		if utils.IsNonCompletedPod(pod) {
			s := gangSpecOf(pod, timeoutConfig, g.gangAnnotationPrefix)
			if spec == nil {
				spec = &s
			} else if s != *spec {
				return false
			}
		}
	}
	return true
}
