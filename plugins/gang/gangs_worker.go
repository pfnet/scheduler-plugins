package gang

import (
	"context"
	"hash/fnv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/klog/v2"
)

type positionAnnotationUpdateTask struct {
	pod      *corev1.Pod
	position PodPosition
}

func (gangs *Gangs) updatePositionAnnotation(ch <-chan positionAnnotationUpdateTask) {
	for task := range ch {
		pod, position := task.pod, task.position
		currentPosision, ok := pod.Annotations[GangSchedulePositionAnnotationKey(gangs.gangAnnotationPrefix)]
		if !ok || currentPosision != position.String() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			applyConfig := corev1apply.Pod(pod.Name, pod.Namespace).WithAnnotations(map[string]string{
				GangSchedulePositionAnnotationKey(gangs.gangAnnotationPrefix): position.String(),
			})
			if _, err := gangs.client.CoreV1().Pods(pod.Namespace).Apply(ctx, applyConfig, metav1.ApplyOptions{FieldManager: "scheduler-gangs"}); err != nil {
				klog.Warningf("failed to apply %v to Pod(%s) : %v", applyConfig, klog.KObj(pod), err)
			}
			cancel()
		}
	}
}

func hashPodUID(uid types.UID) uint32 {
	h := fnv.New32a()
	h.Write([]byte(uid))
	return h.Sum32()
}
