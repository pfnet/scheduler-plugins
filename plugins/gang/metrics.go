package gang

import (
	"sync"

	"k8s.io/component-base/metrics"
)

var (
	// Metrics are registered in NewPlugin().

	gangSchedulingEventCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:      "event_total",
			Namespace: "gang",
			Help:      "Count of gang scheduling events",
		},
		[]string{"type", "namespace", "gang"},
	)

	registerMetrics sync.Once
)

func incEventCounter(event GangSchedulingEvent, namespace, gang string) {
	gangSchedulingEventCounter.With(
		map[string]string{
			"type":      string(event),
			"namespace": namespace,
			"gang":      gang,
		},
	).Inc()
}
