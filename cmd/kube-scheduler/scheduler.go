package main

import (
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/spf13/pflag"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version" // for version metric registration
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	gang "github.pfidev.jp/scheduler-plugins/plugins/gang"
)

var (
	// LDFLAGS should overwrite these variables in build time.
	Version  string = "unknown"
	Revision string = "unknown"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(gang.PluginName, gang.NewPlugin),
	)

	// TODO: once we switch everything over to Cobra commands, we can go back to calling
	// utilflag.InitFlags() (by removing its pflag.Parse() call). For now, we have to set the
	// normalize func and add the go flag set by hand.
	pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	// utilflag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	// Start pprof server
	go func() {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		klog.Info("Starting http server 0.0.0.0:6060 for pprof")
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			klog.Error(err)
		}
	}()

	if err := command.Execute(); err != nil {
		klog.Fatalf("%+v", err)
	}
}
