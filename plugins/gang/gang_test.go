package gang

import (
	"fmt"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pfnet/scheduler-plugins/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const gangAnnotationPrefix = "scheduling.k8s.pfn.io/gang"

var _ = Describe("Gang.String", func() {
	It("should work", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
		}, gangAnnotationPrefix)
		Expect(gang.String()).To(Equal("user-0/gang-0: []"))

		gang.AddOrUpdate(makePod("user-0", "pod-0", v1.PodPending, nil))
		gang.AddOrUpdate(makePod("user-0", "pod-1", v1.PodRunning, nil))

		Expect(gang.String()).To(
			SatisfyAny(
				Equal("user-0/gang-0: [pod-0(Pending, Unknown), pod-1(Running, Unknown)]"),
				Equal("user-0/gang-0: [pod-1(Running, Unknown), pod-0(Pending, Unknown)]"),
			),
		)
	})
})

var _ = Describe("Gang.AddOrUpdate, Delete, CountPod, CountPodIf", func() {
	It("should work", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
		}, gangAnnotationPrefix)
		// Empty gang
		Expect(gang.CountPod()).To(Equal(0))

		// Add a new Pod
		pod := makePod("user-0", "pod-0", v1.PodPending, nil)
		gang.AddOrUpdate(pod)
		Expect(gang.CountPod()).To(Equal(1))
		Expect(gang.CountPodIf(utils.IsAssignedPod)).To(Equal(0))

		// Update the Pod
		pod.Spec.NodeName = "node-0"
		gang.AddOrUpdate(pod)
		Expect(gang.CountPod()).To(Equal(1))
		Expect(gang.CountPodIf(utils.IsAssignedPod)).To(Equal(1))

		// Delete a Pod that is not in the gang
		gang.Delete(makePod("user-0", "pod-1", v1.PodPending, nil))
		Expect(gang.CountPod()).To(Equal(1))
		Expect(gang.CountPodIf(utils.IsAssignedPod)).To(Equal(1))

		// Delete the Pod in the gang
		gang.Delete(pod)
		Expect(gang.CountPod()).To(Equal(0))
		Expect(gang.CountPodIf(utils.IsAssignedPod)).To(Equal(0))
	})
})

var _ = Describe("Gang.SatisfiesInvariantForScheduling", func() {
	It("should work", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
			Spec: GangSpec{Size: 2},
		}, gangAnnotationPrefix)
		timeoutConfig := ScheduleTimeoutConfig{
			DefaultSeconds: 300,
			LimitSeconds:   300,
		}

		// NotReady
		satisfies, status := gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangNotReady))

		// SpecInvalid
		pod0 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-0",
				UID:  types.UID("pod-0"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix):                   "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix):                   "2",
					GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix): "100",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}

		pod1 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-1",
				UID:  types.UID("pod-1"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix): "2",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}

		gang.AddOrUpdate(pod0)
		gang.AddOrUpdate(pod1)

		satisfies, status = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangSpecInvalid))

		// Satisfies invariant
		pod0.Annotations[GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix)] = "300"
		satisfies, _ = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeTrue())

		// FullyScheduled
		pod2 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-2",
				UID:  types.UID("pod-2"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix): "2",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}

		pod0.Spec.NodeName = "node-0"
		pod1.Spec.NodeName = "node-1"
		pod2.Spec.NodeName = "node-2"

		satisfies, status = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangFullyScheduled))

		// WaitForTerminating
		pod0.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		satisfies, status = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangWaitForTerminating))
	})
	It("returns GangNotReady when completed pods don't count toward gang size", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
			Spec: GangSpec{Size: 2},
		}, gangAnnotationPrefix)
		timeoutConfig := ScheduleTimeoutConfig{
			DefaultSeconds: 300,
			LimitSeconds:   300,
		}

		// NotReady
		satisfies, status := gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangNotReady))

		podSucceeded := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-succeeded",
				UID:  types.UID("pod-succeeded"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix):                   "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix):                   "2",
					GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix): "100",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodSucceeded,
			},
		}

		podFailed := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-failed",
				UID:  types.UID("pod-failed"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix): "2",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodFailed,
			},
		}

		gang.AddOrUpdate(podSucceeded)
		gang.AddOrUpdate(podFailed)

		satisfies, status = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeFalse())
		Expect(status).To(Equal(GangNotReady))

		// FullyScheduled
		gangSize := 2
		for i := 0; i < gangSize; i++ {
			podName := fmt.Sprintf("pod-%d", i)
			gang.AddOrUpdate(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
					UID:  types.UID(podName),
					Annotations: map[string]string{
						GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
						GangSizeAnnotationKey(gangAnnotationPrefix): strconv.Itoa(gangSize),
					},
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			})
		}
		satisfies, _ = gang.SatisfiesInvariantForScheduling(timeoutConfig)
		Expect(satisfies).To(BeTrue())
	})
})

var _ = Describe("Gang.IterateOverPods", func() {
	It("should work", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
		}, gangAnnotationPrefix)

		// Empty gang
		names := []string{}
		gang.IterateOverPods(func(p *v1.Pod) {
			names = append(names, p.Name)
		})
		Expect(names).To(BeEmpty())

		// Non-mutating function
		gang.AddOrUpdate(makePod("user-0", "pod-0", v1.PodPending, nil))
		gang.AddOrUpdate(makePod("user-0", "pod-1", v1.PodPending, nil))

		gang.IterateOverPods(func(p *v1.Pod) {
			names = append(names, p.Name)
		})
		Expect(names).To(SatisfyAny(
			Equal([]string{"pod-0", "pod-1"}),
			Equal([]string{"pod-1", "pod-0"}),
		))

		// Mutating function
		gang.IterateOverPods(func(p *v1.Pod) {
			p.Name = p.Name + "-mutated"
		})

		names = []string{}
		gang.IterateOverPods(func(p *v1.Pod) {
			names = append(names, p.Name)
		})
		Expect(names).To(SatisfyAny(
			Equal([]string{"pod-0-mutated", "pod-1-mutated"}),
			Equal([]string{"pod-1-mutated", "pod-0-mutated"}),
		))
	})
})

var _ = Describe("Gang.IsAllNonCompletedSpecIdenticalTo", func() {
	It("should work", func() {
		gang := NewGang(GangNameAndSpec{
			Name: GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"}),
		}, gangAnnotationPrefix)

		spec := GangSpec{
			Size:                 2,
			TimeoutBase:          300 * time.Second,
			TimeoutJitterSeconds: 100,
		}
		timeoutConfig := ScheduleTimeoutConfig{
			DefaultSeconds: 300,
			LimitSeconds:   300,
			JitterSeconds:  100,
		}

		// Empty gang
		Expect(gang.IsAllNonCompletedSpecIdenticalTo(spec, timeoutConfig)).To(BeTrue())

		// One Pod
		pod0 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-0",
				UID:  types.UID("pod-0"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix):                   "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix):                   "2",
					GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix): "100",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}

		gang.AddOrUpdate(pod0)
		Expect(gang.IsAllNonCompletedSpecIdenticalTo(spec, timeoutConfig)).To(BeFalse())

		pod0.Annotations[GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix)] = "300"
		Expect(gang.IsAllNonCompletedSpecIdenticalTo(spec, timeoutConfig)).To(BeTrue())

		// Two Pod
		pod1 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-1",
				UID:  types.UID("pod-1"),
				Annotations: map[string]string{
					GangNameAnnotationKey(gangAnnotationPrefix):                   "gang-0",
					GangSizeAnnotationKey(gangAnnotationPrefix):                   "2",
					GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix): "100",
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}

		gang.AddOrUpdate(pod1)
		Expect(gang.IsAllNonCompletedSpecIdenticalTo(spec, timeoutConfig)).To(BeFalse())

		pod1.Annotations[GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix)] = "300"
		Expect(gang.IsAllNonCompletedSpecIdenticalTo(spec, timeoutConfig)).To(BeTrue())
	})
})
