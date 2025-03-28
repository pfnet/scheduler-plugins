package mocks

import (
	"context"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

// Implements framework.Handle
type MockFrameworkHandle struct {
	clientset   clientset.Interface
	WaitingPods map[types.UID]framework.WaitingPod
}

// Implements framework.WaitingPod
type MockWaitingPod struct {
	Pod *v1.Pod
}

var _ framework.Handle = &MockFrameworkHandle{}
var _ framework.WaitingPod = &MockWaitingPod{}

func (f *MockFrameworkHandle) SnapshotSharedLister() framework.SharedLister {
	return nil
}

func (f *MockFrameworkHandle) ResourceClaimCache() *assumecache.AssumeCache {
	return nil
}

func (f *MockFrameworkHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	for _, wp := range f.WaitingPods {
		callback(wp)
	}
}

func (f *MockFrameworkHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	return f.WaitingPods[uid]
}

func (f *MockFrameworkHandle) RejectWaitingPod(uid types.UID) bool {
	if w := f.GetWaitingPod(uid); w != nil {
		w.Reject("dummy", "removed")
		return true
	}
	return false
}

// framework.PodNominator
func (f *MockFrameworkHandle) AddNominatedPod(logger logr.Logger, pod *framework.PodInfo, nominatingInfo *framework.NominatingInfo) {
}
func (f *MockFrameworkHandle) DeleteNominatedPodIfExists(pod *v1.Pod) {}
func (f *MockFrameworkHandle) UpdateNominatedPod(logger logr.Logger, oldPod *v1.Pod, newPodInfo *framework.PodInfo) {
}
func (f *MockFrameworkHandle) NominatedPodsForNode(nodeName string) []*framework.PodInfo { return nil }

// framework.PluginsRunner
func (f *MockFrameworkHandle) RunPreScorePlugins(context.Context, *framework.CycleState, *v1.Pod, []*framework.NodeInfo) *framework.Status {
	return nil
}
func (f *MockFrameworkHandle) RunScorePlugins(context.Context, *framework.CycleState, *v1.Pod, []*framework.NodeInfo) ([]framework.NodePluginScores, *framework.Status) {
	return nil, nil
}
func (f *MockFrameworkHandle) RunFilterPlugins(context.Context, *framework.CycleState, *v1.Pod, *framework.NodeInfo) *framework.Status {
	return nil
}
func (f *MockFrameworkHandle) RunPreFilterExtensionAddPod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return nil
}
func (f *MockFrameworkHandle) RunPreFilterExtensionRemovePod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return nil
}

// framework.Handle
func (f *MockFrameworkHandle) KubeConfig() *restclient.Config { return nil }
func (f *MockFrameworkHandle) ClientSet() clientset.Interface {
	if f.clientset == nil {
		f.clientset = fake.NewSimpleClientset()
	}
	return f.clientset
}
func (f *MockFrameworkHandle) EventRecorder() events.EventRecorder                    { return &events.FakeRecorder{} }
func (f *MockFrameworkHandle) SharedInformerFactory() informers.SharedInformerFactory { return nil }
func (f *MockFrameworkHandle) RunFilterPluginsWithNominatedPods(ctx context.Context, state *framework.CycleState, pod *v1.Pod, info *framework.NodeInfo) *framework.Status {
	return nil
}
func (f *MockFrameworkHandle) Extenders() []framework.Extender { return nil }
func (f *MockFrameworkHandle) Parallelizer() parallelize.Parallelizer {
	return parallelize.NewParallelizer(1)
}

func (w *MockWaitingPod) GetPod() *v1.Pod             { return w.Pod }
func (w *MockWaitingPod) GetPendingPlugins() []string { return []string{} }
func (w *MockWaitingPod) Allow(pluginName string)     {}
func (w *MockWaitingPod) Reject(plugin, msg string)   {}

func (f *MockFrameworkHandle) AddWaitingPod(uid types.UID, wp framework.WaitingPod) {
	f.WaitingPods[uid] = wp
}

func (f *MockFrameworkHandle) RemoveWaitingPod(uid types.UID) {
	delete(f.WaitingPods, uid)
}
