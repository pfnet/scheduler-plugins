package gang

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pfnet/scheduler-plugins/mocks"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8swait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

var _ = Describe("PreEnqueue", func() {
	It("The first Pod on gang and the gang isn't registered", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)

		pod.Namespace = "user-0"
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}
		status := gangs.PreEnqueue(pod)

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
		Expect(status.Code()).To(Equal(framework.Unschedulable))
	})
	It("The Pod position is PodPositionUnknown", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		pod.Status.Phase = v1.PodSucceeded
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionUnknown)

		status := gangs.PreEnqueue(pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
		Expect(status.Code()).To(Equal(framework.Unschedulable))
	})
	It("The Pod position is PodPositionActiveQ and all gang Pods are created", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod1, pod2 := makePod("user-0", "pod-1", v1.PodPending, fakeClient), makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		pod1.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}
		pod2.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 2}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)
		gangs.gangs[gn].PutPosition(pod1, PodPositionActiveQ)
		gangs.gangs[gn].PutPosition(pod2, PodPositionActiveQ)

		status := gangs.PreEnqueue(pod1)
		Expect(status.Code()).To(Equal(framework.Success))

		p := gangs.gangs[gn].GetPosition(pod1.UID)
		Expect(p).To(Equal(PodPositionActiveQ))
	})
	It("The Pod position is PodPositionSchedulingCycle and all gang Pods are created", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod1, pod2 := makePod("user-0", "pod-1", v1.PodPending, fakeClient), makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		pod1.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}
		pod2.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 2}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)
		gangs.gangs[gn].PutPosition(pod1, PodPositionSchedulingCycle)
		gangs.gangs[gn].PutPosition(pod2, PodPositionSchedulingCycle)

		status := gangs.PreEnqueue(pod1)

		p := gangs.gangs[gn].GetPosition(pod1.UID)
		Expect(p).To(Equal(PodPositionActiveQ))
		Expect(status.Code()).To(Equal(framework.Success))
	})
	It("The Pod position is PodPositionSchedulingCycle but the number of Pods doesn't meet gang spec", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod1.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2", // size is 2, but only 1 Pod is created
		}
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 2}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].PutPosition(pod1, PodPositionSchedulingCycle)

		status := gangs.PreEnqueue(pod1)

		p := gangs.gangs[gn].GetPosition(pod1.UID)
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))
		Expect(status.Code()).To(Equal(framework.UnschedulableAndUnresolvable))

		// when PreEnqueue rejects a Pod due to GangSpecInvalid, the Pod is inserted to podUIDs
		Expect(gangs.podUIDs.Has("user-0/pod-1")).To(Equal(true))
	})
	It("The Pod position is PodPositionUnschedulablePodPool and all other gang Pods are PodPositionReadyToSchedule", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod2 := makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		for _, p := range []*v1.Pod{pod, pod1, pod2} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "3",
			}
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionUnschedulablePodPool)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionReadyToSchedule)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "user-0", UID: "user-0/pod-2"}}, PodPositionReadyToSchedule)

		status := gangs.PreEnqueue(pod)
		Expect(len(gangs.activateGangsPool)).To(Equal(1))
		_, ok := gangs.activateGangsPool[GangName{Namespace: "user-0", Name: "gang-0"}]
		Expect(ok).To(Equal(true))

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
		Expect(status.Code()).To(Equal(framework.Unschedulable))
	})
	It("The Pod position is PodPositionUnschedulablePodPool and not all other gang Pods are PodPositionReadyToSchedule", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod2 := makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		for _, p := range []*v1.Pod{pod, pod1, pod2} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "3",
			}
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionUnschedulablePodPool)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionReadyToSchedule)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "user-0", UID: "user-0/pod-2"}}, PodPositionUnschedulablePodPool)

		status := gangs.PreEnqueue(pod)
		Expect(len(gangs.activateGangsPool)).To(Equal(0))

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
		Expect(status.Code()).To(Equal(framework.Unschedulable))
	})
})

var _ = Describe("PostFilter", func() {
	It("do nothing with no gang Pod", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		gangs.mapLock.Lock() // Lock it so that the test case will fail with timeout if PostFilter tries to do something.
		gangs.PostFilter(context.Background(), pod)
	}, 1)
	It("the position for the pod isn't registered", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		pod.Status.Phase = v1.PodSucceeded
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.PostFilter(context.Background(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))
	})
	It("the position for the pod is PodPositionReadyToSchedule", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		pod.Status.Phase = v1.PodSucceeded
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionReadyToSchedule)

		gangs.PostFilter(context.Background(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
	})
	It("the position for the pod isn't PodPositionReadyToSchedule", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		pod.Status.Phase = v1.PodSucceeded
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionSchedulingCycle)
		gangs.PostFilter(context.Background(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))
	})
	It("when any Pod in the same gang has NominatedNodeName, NominatedNodeName will be removed", func() {
		fwh := &mocks.MockFrameworkHandle{}
		client := fwh.ClientSet()
		gangs := NewGangs(context.Background(), fwh, client, ScheduleTimeoutConfig{}, gangAnnotationPrefix)

		existingPod := makePod("user-0", "pod-1", v1.PodPending, client)
		existingPod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}
		existingPod.Status.NominatedNodeName = "nominated"
		_, err := client.CoreV1().Pods("user-0").Update(context.Background(), existingPod, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		pod := makePod("user-0", "pod-0", v1.PodPending, client)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		pod.Status.Phase = v1.PodPending
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(existingPod)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionSchedulingCycle)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionSchedulingCycle)
		gangs.PostFilter(context.Background(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))

		pod, err = fwh.ClientSet().CoreV1().Pods("user-0").Get(context.Background(), "pod-1", metav1.GetOptions{})
		Expect(pod.Status.NominatedNodeName).To(Equal(""))
		Expect(err).ShouldNot(HaveOccurred())

		p1 := gangs.gangs[gn].GetPosition("user-0/pod-1")
		Expect(p1).To(Equal(PodPositionReadyToSchedule))
	})
	It("when there is waiting Pod in the same gang, position will be changed to PodPositionReadyToSchedule", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(makePod("user-0", "pod-1", v1.PodPending, fakeClient))
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionSchedulingCycle)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionWaitingOnPermit)
		gangs.PostFilter(context.Background(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))

		p1 := gangs.gangs[gn].GetPosition("user-0/pod-1")
		Expect(p1).To(Equal(PodPositionReadyToSchedule))
	})
})

var _ = Describe("PreFilter", func() {
	It("do nothing with no gang Pod", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		gangs.mapLock.Lock() // Lock it so that the test case will fail with timeout if PreFilter tries to do something.
		status := gangs.PreFilter(context.Background(), nil, pod)
		Expect(status.Code()).To(Equal(framework.Success))
	}, 1)
	It("the gang isn't registered", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		status := gangs.PreFilter(context.Background(), nil, pod)

		Expect(status.Code()).To(Equal(framework.UnschedulableAndUnresolvable))
	})
	It("the position for another pod in the same gang is PodPositionUnschedulablePodPool", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod2 := makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		for _, p := range []*v1.Pod{pod, pod1, pod2} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "3",
			}
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionActiveQ)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionUnschedulablePodPool)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "user-0", UID: "user-0/pod-2"}}, PodPositionActiveQ)

		status := gangs.PreFilter(context.Background(), nil, pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionReadyToSchedule))
		Expect(status.Code()).To(Equal(framework.UnschedulableAndUnresolvable))
	})
	It("the position for all other pod in the same gang is PodPositionActiveQ", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{JitterSeconds: 1})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod2 := makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		for _, p := range []*v1.Pod{pod, pod1, pod2} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "3",
			}
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 3, TimeoutJitterSeconds: 1}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionActiveQ)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionActiveQ)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "user-0", UID: "user-0/pod-2"}}, PodPositionActiveQ)

		status := gangs.PreFilter(context.Background(), framework.NewCycleState(), pod)

		p := gangs.gangs[gn].GetPosition("user-0/pod-0")
		Expect(p).To(Equal(PodPositionSchedulingCycle))
		Expect(status.Code()).To(Equal(framework.Success))
	})
})

var _ = Describe("Permit", func() {
	It("if return status is success, all gangs in activateGangsPool will be activated", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{JitterSeconds: 1})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		pod2 := makePod("user-0", "pod-2", v1.PodPending, fakeClient)
		for _, p := range []*v1.Pod{pod, pod1, pod2} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "3",
			}
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 3, TimeoutJitterSeconds: 1}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn].AddOrUpdate(pod2)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "user-0", UID: "user-0/pod-0"}}, PodPositionReadyToSchedule)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionReadyToSchedule)
		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "user-0", UID: "user-0/pod-2"}}, PodPositionReadyToSchedule)

		gangs.activateGangsPool.Insert(gn)

		state := framework.NewCycleState()
		podsToActivate := framework.NewPodsToActivate()
		state.Write(framework.PodsToActivateKey, podsToActivate)

		status, _ := gangs.Permit(state, makePod("user-1", "incoming", v1.PodPending, fakeClient))

		Expect(status.Code()).To(Equal(framework.Success))
		Expect(len(gangs.activateGangsPool)).To(Equal(0))
		for _, uid := range []string{"user-0/pod-0", "user-0/pod-1", "user-0/pod-2"} {
			p := gangs.gangs[gn].GetPosition(types.UID(uid))
			Expect(p).To(Equal(PodPositionActiveQ))
		}

		c, err := state.Read(framework.PodsToActivateKey)
		typed, ok := c.(*framework.PodsToActivate)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(ok).To(Equal(true))
		Expect(typed.Map).To(Equal(map[string]*v1.Pod{"user-0/pod-0": pod, "user-0/pod-1": pod1, "user-0/pod-2": pod2}))
	})
})

var _ = Describe("AddOrUpdate", func() {
	It("if a gang Pod is added, the positions of each gang pod in podUIDs will be changed", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{JitterSeconds: 1})
		pod0 := makePod("user-0", "pod-0", v1.PodPending, fakeClient)
		pod1 := makePod("user-0", "pod-1", v1.PodPending, fakeClient)
		podA := makePod("user-1", "pod-a", v1.PodPending, fakeClient)

		for _, p := range []*v1.Pod{pod0, pod1} {
			p.Annotations = map[string]string{
				GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				GangSizeAnnotationKey(gangAnnotationPrefix): "2",
			}
		}

		podA.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-1",
			GangSizeAnnotationKey(gangAnnotationPrefix): "2",
		}

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gn1 := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-1"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 2, TimeoutJitterSeconds: 1}}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(pod1)
		gangs.gangs[gn1] = NewGang(GangNameAndSpec{Name: gn1, Spec: GangSpec{Size: 2, TimeoutJitterSeconds: 1}}, gangAnnotationPrefix)
		gangs.gangs[gn1].AddOrUpdate(podA)

		gangs.gangs[gn].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "user-0", UID: "user-0/pod-1"}}, PodPositionUnschedulablePodPool)
		gangs.gangs[gn1].PutPosition(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "user-1", UID: "user-1/pod-a"}}, PodPositionUnschedulablePodPool)

		gangs.podUIDs.Insert("user-0/pod-1", "user-1/pod-a")

		gangs.AddOrUpdate(pod0, &events.FakeRecorder{})

		Expect(gangs.podUIDs.Has("user-0/pod-1")).To(Equal(false))
		Expect(gangs.podUIDs.Has("user-1/pod-a")).To(Equal(true))

		p := gangs.gangs[gn].GetPosition("user-0/pod-1")
		Expect(p).To(Equal(PodPositionReadyToSchedule))

		p = gangs.gangs[gn1].GetPosition("user-1/pod-a")
		Expect(p).To(Equal(PodPositionUnschedulablePodPool))
	})
})

var _ = Describe("PositionAnnotation", func() {
	It("an annotation should be updated after putPosition", func() {
		gangs, fakeClient := NewGangsForTest(&mocks.MockFrameworkHandle{}, ScheduleTimeoutConfig{})
		pod := makePod("user-0", "pod-0", v1.PodPending, fakeClient)

		gn := GangName(types.NamespacedName{Namespace: pod.Namespace, Name: "gang-0"})
		gang := NewGang(GangNameAndSpec{Name: gn, Spec: GangSpec{Size: 2, TimeoutJitterSeconds: 1}}, gangAnnotationPrefix)
		var (
			updatedPod *v1.Pod
			err        error
		)

		for _, targetPoision := range []PodPosition{PodPositionReadyToSchedule, PodPositionActiveQ, PodPositionSchedulingCycle, PodPositionUnschedulablePodPool, PodPositionWaitingOnPermit} {
			gangs.putPosition(gang, pod, targetPoision)

			err = k8swait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 10*time.Second, true, func(context.Context) (done bool, err error) {
				updatedPod, err = fakeClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				if err != nil {
					return true, err
				}

				return updatedPod.Annotations[GangSchedulePositionAnnotationKey(gangAnnotationPrefix)] == targetPoision.String(), nil
			})
			Expect(err).ShouldNot(HaveOccurred())
		}
	})
})

func NewGangsForTest(fwh framework.Handle, timeoutConfig ScheduleTimeoutConfig) (*Gangs, *fake.Clientset) {
	fakeClient := fake.NewSimpleClientset()
	gangs := NewGangs(context.Background(), fwh, fakeClient, timeoutConfig, gangAnnotationPrefix)
	return gangs, fakeClient
}
