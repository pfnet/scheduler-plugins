package gang

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("GangName", func() {
	It("GangNameOf", func() {
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "user-0",
				Name:      "pod-0",
			},
		}

		_, ok := GangNameOf(&pod, gangAnnotationPrefix)
		Expect(ok).To(BeFalse())

		pod.Annotations = map[string]string{
			GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
		}

		gn, ok := GangNameOf(&pod, gangAnnotationPrefix)
		Expect(ok).To(BeTrue())
		Expect(gn).To(Equal(GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})))
	})

	It("String", func() {
		gn := GangName(types.NamespacedName{Namespace: "user-0", Name: "gang-0"})
		Expect(gn.String()).To(Equal("user-0/gang-0"))
	})
})

var _ = Describe("GangSpec", func() {
	It("gangSpecOf", func() {
		timeoutConfig := ScheduleTimeoutConfig{
			DefaultSeconds: 100,
			LimitSeconds:   300,
			JitterSeconds:  100,
		}

		// Default timeout seconds
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "user-0",
				Name:      "pod-0",
				Annotations: map[string]string{
					GangSizeAnnotationKey(gangAnnotationPrefix): "2",
					GangNameAnnotationKey(gangAnnotationPrefix): "gang-0",
				},
			},
		}

		Expect(gangSpecOf(&pod, timeoutConfig, gangAnnotationPrefix)).To(Equal(GangSpec{
			Size:                 2,
			TimeoutBase:          100 * time.Second,
			TimeoutJitterSeconds: 100,
		}))

		// Specified timeout seconds
		pod.Annotations[GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix)] = "200"
		Expect(gangSpecOf(&pod, timeoutConfig, gangAnnotationPrefix)).To(Equal(GangSpec{
			Size:                 2,
			TimeoutBase:          200 * time.Second,
			TimeoutJitterSeconds: 100,
		}))

		// Limited timeout seconds
		pod.Annotations[GangScheduleTimeoutSecondsAnnotationKey(gangAnnotationPrefix)] = "400"
		Expect(gangSpecOf(&pod, timeoutConfig, gangAnnotationPrefix)).To(Equal(GangSpec{
			Size:                 2,
			TimeoutBase:          300 * time.Second,
			TimeoutJitterSeconds: 100,
		}))
	})

	// GangSizeOf and gangScheduleTimeout are tested as part of gangSpecOf
})
