package gang

import (
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type GangName types.NamespacedName

func GangNameOf(pod *v1.Pod, annotationPrefix string) (GangName, bool) {
	annotations := pod.GetAnnotations()
	if name, ok := annotations[GangNameAnnotationKey(annotationPrefix)]; ok {
		return GangName{Namespace: pod.GetNamespace(), Name: name}, true
	} else {
		return GangName{}, false
	}
}

func (gn GangName) String() string {
	return (types.NamespacedName)(gn).String()
}

type GangSpec struct {
	Size                 int
	TimeoutBase          time.Duration
	TimeoutJitterSeconds int
}

func gangSpecOf(pod *v1.Pod, config ScheduleTimeoutConfig, annotationPrefix string) GangSpec {
	return GangSpec{
		Size:                 GangSizeOf(pod, annotationPrefix),
		TimeoutBase:          gangScheduleTimeout(pod, config, annotationPrefix),
		TimeoutJitterSeconds: config.JitterSeconds,
	}
}

// TODO: Return (int, error) or (int, bool) to handle error
func GangSizeOf(pod *v1.Pod, annotationPrefix string) int {
	sizeStr, ok := pod.GetAnnotations()[GangSizeAnnotationKey(annotationPrefix)]
	if !ok {
		return 0
	}
	if size, err := strconv.Atoi(sizeStr); err != nil {
		return 0
	} else {
		return size
	}
}

func gangScheduleTimeout(pod *v1.Pod, config ScheduleTimeoutConfig, annotationPrefix string) time.Duration {
	seconds := func() int {
		str, ok := pod.GetAnnotations()[GangScheduleTimeoutSecondsAnnotationKey(annotationPrefix)]
		if !ok {
			return config.DefaultSeconds
		}

		sec, err := strconv.Atoi(str)
		if err != nil {
			return config.DefaultSeconds
		}

		if sec < config.LimitSeconds {
			return sec
		}
		return config.LimitSeconds
	}

	return time.Duration(seconds()) * time.Second
}

type GangNameAndSpec struct {
	Name GangName
	Spec GangSpec
}

func GangNameAndSpecOf(pod *v1.Pod, config ScheduleTimeoutConfig, gangAnnotationPrefix string) (GangNameAndSpec, bool) {
	if !IsGang(pod, gangAnnotationPrefix) {
		return GangNameAndSpec{}, false
	}
	name, _ := GangNameOf(pod, gangAnnotationPrefix)
	return GangNameAndSpec{
		Name: name,
		Spec: gangSpecOf(pod, config, gangAnnotationPrefix),
	}, true
}

func IsGang(pod *v1.Pod, gangAnnotationPrefix string) bool {
	_, nameOK := GangNameOf(pod, gangAnnotationPrefix)
	// Gang of size = 1 is actually not a gang
	return nameOK && GangSizeOf(pod, gangAnnotationPrefix) > 1
}
