package gang

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Gangs.String", func() {
	It("should format gangs into a correct string", func() {
		gangs := NewGangs(nil, nil, ScheduleTimeoutConfig{}, gangAnnotationPrefix)
		Expect(gangs.String()).To(Equal("{}"))

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(makePod("pod-0", "user-0/pod-0", v1.PodPending))
		gangs.gangs[gn].AddOrUpdate(makePod("pod-1", "user-0/pod-1", v1.PodRunning))

		gn = GangName(types.NamespacedName{Namespace: "user-1", Name: "gang-1"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(makePod("pod-0", "user-1/pod-0", v1.PodSucceeded))

		Expect(gangs.String()).To(
			SatisfyAny(
				Equal("{user-0/gang-0: [pod-0(Pending), pod-1(Running)], user-1/gang-1: [pod-0(Succeeded)]}"),
				Equal("{user-0/gang-0: [pod-1(Running), pod-0(Pending)], user-1/gang-1: [pod-0(Succeeded)]}"),
				Equal("{user-1/gang-1: [pod-0(Succeeded)], user-0/gang-0: [pod-0(Pending), pod-1(Running)]}"),
				Equal("{user-1/gang-1: [pod-0(Succeeded)], user-0/gang-0: [pod-1(Running), pod-0(Pending)]}"),
			),
		)
	})
})

func makePod(name, uid string, phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(uid),
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
}
