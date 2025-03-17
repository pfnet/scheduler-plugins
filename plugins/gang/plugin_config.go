package gang

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

type PluginConfig struct {
	// GangAnnotationPrefix is the prefix of all gang annotations.
	// This configuration is required and if not set, the plugin will return an error during its initialization.
	GangAnnotationPrefix string `json:"gangAnnotationPrefix"`

	SchedulerName                     string `json:"schedulerName,omitempty"`
	GangScheduleTimeoutSecondsLimit   int    `json:"gangScheduleTimeoutSecondLimit,omitempty"`
	GangScheduleDefaultTimeoutSeconds int    `json:"gangScheduleDefaultTimeoutSecond,omitempty"`
	GangScheduleTimeoutJitterSeconds  int    `json:"gangScheduleTimeoutJitterSecond,omitempty"`

	HealthCheckAddr string `json:"healthCheckAddr,omitempty"`
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
	if config.GangScheduleTimeoutSecondsLimit <= 0 {
		config.GangScheduleTimeoutSecondsLimit = GangScheduleTimeoutSecondsLimitDefault
	}
	if config.GangScheduleDefaultTimeoutSeconds <= 0 {
		config.GangScheduleDefaultTimeoutSeconds = GangScheduleTimeoutSecondsDefault
	}
	if config.GangScheduleTimeoutJitterSeconds <= 0 {
		config.GangScheduleTimeoutJitterSeconds = GangScheduleTimeoutJitterSecondsDefault
	}
	if config.SchedulerName == "" {
		config.SchedulerName = v1.DefaultSchedulerName
	}
}
