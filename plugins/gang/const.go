package gang

import "github.com/pfnet/scheduler-plugins/plugins/names"

const (
	// PluginName is the name of the plugin.
	PluginName = names.Gang

	// GangScheduleTimeoutSecondsDefault is the default value for gang schedule timeout
	GangScheduleTimeoutSecondsDefault = 30

	// GangScheduleTimeoutSecondsLimitDefault is the default value of the upper limit value
	// for gang schedule timeout which user can define.
	// If user defines the value over this limit, scheduler apply the upper limit implicitly
	// This value will be used unless Plugin.GangScheduleTimeoutSecondsLimitDefault is set.
	GangScheduleTimeoutSecondsLimitDefault = 100

	// GangScheduleTimeoutJitterSecondsDefault is the default value of maximum jitter of gang
	// schedule timeout.
	// Timeout seconds of a gang scheduling will be calculated as:
	//   user-specified timeout + random value sampled from [0, maximum jitter)
	GangScheduleTimeoutJitterSecondsDefault = GangScheduleTimeoutSecondsDefault

	// StateKeyGangFirstPod is a key of CycleState.
	// Gang PreFilter plugin writes a value with this key if a given pod is the first of a new gang.
	// UniqueZone PreFilter plugin later reads CycleState with this key.
	StateKeyGangFirstPod = "GangFirstPod"
)

type GangSchedulingEvent string

const (
	// GangNotReady means the number of gang Pods isn't sufficient.
	GangNotReady GangSchedulingEvent = "GangNotReady"
	// GangNotReadyToSchedule means the number of gang Pods is sufficient, but not all Pods are ready to schedule,
	// which technically means some gang Pods are rejected by scheduler plugins other than gang in the scheduling cycle.
	GangNotReadyToSchedule   GangSchedulingEvent = "GangNotReadyToSchedule"
	GangSpecInvalid          GangSchedulingEvent = "GangSpecInvalid"
	GangWaitForTerminating   GangSchedulingEvent = "GangWaitForTerminating"
	GangFullyScheduled       GangSchedulingEvent = "GangFullyScheduled"
	GangWaitForReady         GangSchedulingEvent = "GangWaitForReady"
	GangSchedulingTimedOut   GangSchedulingEvent = "GangSchedulingTimedOut"
	GangReady                GangSchedulingEvent = "GangReady"
	GangOtherPodGetsRejected GangSchedulingEvent = "GangOtherPodGetsRejected"
	FillingRunningGang       GangSchedulingEvent = "FillingRunningGang"
	DeletedAsPartOfGang      GangSchedulingEvent = "DeletedAsPartOfGang"
)
