package gang

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

var _ = Describe("Gangs.String", func() {
	It("should format gangs into a correct string", func() {
		gangs := NewGangs(nil, nil, ScheduleTimeoutConfig{}, gangAnnotationPrefix)
		Expect(gangs.String()).To(Equal("{}"))

		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(makePod("user-0", "pod-0", v1.PodPending, nil))
		gangs.gangs[gn].AddOrUpdate(makePod("user-0", "pod-1", v1.PodRunning, nil))

		gn = GangName(types.NamespacedName{Namespace: "user-1", Name: "gang-1"})
		gangs.gangs[gn] = NewGang(GangNameAndSpec{Name: gn}, gangAnnotationPrefix)
		gangs.gangs[gn].AddOrUpdate(makePod("user-1", "pod-0", v1.PodSucceeded, nil))

		Expect(gangs.String()).To(
			SatisfyAny(
				Equal("{user-0/gang-0: [pod-0(Pending, Unknown), pod-1(Running, Unknown)], user-1/gang-1: [pod-0(Succeeded, Unknown)]}"),
				Equal("{user-0/gang-0: [pod-1(Running, Unknown), pod-0(Pending, Unknown)], user-1/gang-1: [pod-0(Succeeded, Unknown)]}"),
				Equal("{user-1/gang-1: [pod-0(Succeeded, Unknown)], user-0/gang-0: [pod-0(Pending, Unknown), pod-1(Running, Unknown)]}"),
				Equal("{user-1/gang-1: [pod-0(Succeeded, Unknown)], user-0/gang-0: [pod-1(Running, Unknown), pod-0(Pending, Unknown)]}"),
			),
		)
	})
})

func makePod(namespace, name string, phase v1.PodPhase, client kubernetes.Interface) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s/%s", namespace, name)),
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}

	if client != nil {
		_, err := client.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
	}

	return pod
}
