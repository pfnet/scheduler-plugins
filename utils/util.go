package utils

import (
	"fmt"

	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/json"
)

func ResponsibleForPod(pod *v1.Pod, schedulerName string) bool {
	return schedulerName == pod.Spec.SchedulerName
}

func IsAssignedAndNonCompletedPod(pod *v1.Pod) bool {
	return IsAssignedPod(pod) && IsNonCompletedPod(pod)
}

func IsAssignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func IsCompletedPod(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed
}

func IsNonCompletedPod(pod *v1.Pod) bool {
	return !IsCompletedPod(pod)
}

func IsTerminatingPod(pod *v1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

func MustMarshalToJson(obj interface{}) []byte {
	marshaled, err := json.Marshal(obj)
	if err != nil {
		panic(fmt.Sprintf("%+v", errors.Wrapf(err, "can't marshal to json")))
	}
	return marshaled
}
