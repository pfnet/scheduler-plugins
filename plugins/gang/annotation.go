package gang

// GangNameAnnotationKey is the annotation key to define gangs.
// Scheduler recognizes the pod belongs to gang "__gang_name__" in your namespace.
func GangNameAnnotationKey(prefix string) string {
	return prefix + "-name"
}

// GangSizeAnnotationKey is the annotation key to define size of the gang.
// Scheduler waits until the number pods which belongs to the gang specified are created.
func GangSizeAnnotationKey(prefix string) string {
	return prefix + "-size"
}

// GangScheduleTimeoutSecondsAnnotationKey is the annotation key to define schedule timeout of the gang.
// If all the pods in the gang are not scheduled in this time period,
// scheduler mark all the pods in the gang as 'unschedulable' and try to schedule another gang.
func GangScheduleTimeoutSecondsAnnotationKey(prefix string) string {
	return prefix + "-schedule-timeout-seconds"
}
