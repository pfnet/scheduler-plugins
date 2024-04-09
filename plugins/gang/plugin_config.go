package gang

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

type PluginConfig struct {
	// GangAnnotationPrefix is the prefix of all gang annotations.
	// This configuration is required; if not set, the plugin will return an error during its initialization.
	GangAnnotationPrefix string `json:"gangAnnotationPrefix"`
	// SchedulerName is the name of the scheduler.
	// This field is optional; if not set, the default scheduler name will be used.
	SchedulerName string `json:"schedulerName,omitempty"`
	// GangScheduleTimeoutSecondsLimit is the maximum timeout in seconds for gang scheduling.
	// If the timeout configured in the pod annotation exceeds this limit, the timeout will be set to this limit.
	// This field is optional; if not set, 100 will be used as a default value.
	GangScheduleTimeoutSecondsLimit int `json:"gangScheduleTimeoutSecondLimit,omitempty"`
	// GangScheduleDefaultTimeoutSeconds is the default timeout in seconds,
	// which will be used if the timeout is not set in the pod annotation.
	// This field is optional; if not set, 30 will be used as a default value.
	GangScheduleDefaultTimeoutSeconds int `json:"gangScheduleDefaultTimeoutSecond,omitempty"`
	// GangScheduleTimeoutJitterSeconds is the jitter in seconds for timeout.
	// This field is optional; if not set, 30 will be used as a default value.
	GangScheduleTimeoutJitterSeconds int `json:"gangScheduleTimeoutJitterSecond,omitempty"`
}

type ScheduleTimeoutConfig struct {
	DefaultSeconds int
	LimitSeconds   int
	JitterSeconds  int
}

func DecodePluginConfig(configuration runtime.Object) (*PluginConfig, error) {
	config := &PluginConfig{}
	if err := fwkruntime.DecodeInto(configuration, &config); err != nil {
		return nil, fmt.Errorf("failed to decode into %s PluginConfig", PluginName)
	}

	if config.GangAnnotationPrefix == "" {
		return nil, fmt.Errorf("gangAnnotationPrefix is a required configuration")
	}

	config.fillEmptyFields()

	return config, nil
}

func (config *PluginConfig) TimeoutConfig() ScheduleTimeoutConfig {
	return ScheduleTimeoutConfig{
		DefaultSeconds: config.GangScheduleDefaultTimeoutSeconds,
		LimitSeconds:   config.GangScheduleTimeoutSecondsLimit,
		JitterSeconds:  config.GangScheduleTimeoutJitterSeconds,
	}
}

func (config *PluginConfig) fillEmptyFields() {
	if config.SchedulerName == "" {
		config.SchedulerName = corev1.DefaultSchedulerName
	}

	if config.GangScheduleTimeoutSecondsLimit <= 0 {
		config.GangScheduleTimeoutSecondsLimit = GangScheduleTimeoutSecondsLimitDefault
	}
	if config.GangScheduleDefaultTimeoutSeconds <= 0 {
		config.GangScheduleDefaultTimeoutSeconds = GangScheduleTimeoutSecondsDefault
	}
	if config.GangScheduleTimeoutJitterSeconds <= 0 {
		config.GangScheduleTimeoutJitterSeconds = GangScheduleTimeoutJitterSecondsDefault
	}
}
