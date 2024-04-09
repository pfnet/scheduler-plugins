## Gang

This is Gang plugin, aiming to achieve an efficient scheduling for groups of your Pods.

_Gang is still in the early stage, subject to get a breaking change for some reason._

### Motivation

In PFN, we need to schedule a group of Pods at the same time for machine learning.

In in-tree scheduler plugins, there's no plugin that can ensure that all Pods are scheduled at the same time.
In [kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins/tree/master/pkg/coscheduling),
they have [coscheduling plugin](https://github.com/kubernetes-sigs/scheduler-plugins/tree/master/pkg/coscheduling),
which is very similar idea to this gang plugin.

However, they've not improved much in recent years while there's many parts to be improved/fixed.

Therefore, we founded gang plugin here:
- Simple configuration: It doesn't require any CRD, simply configured by Pod annotations.
- **Enhanced requeueing**: The gang plugin tracks why each Pod in a gang is rejected and requeue Pods only when all Pods in gang are ready. It contributes to the efficiency of scheduling and preventing many Pods from reserving resources in vain.

### Usage

Gang is made of quite simple configuration;
all the configuration is done within Pod's anntoations and doesn't require any CRD installed.

```yaml
metadata:
  generateName: gangpod-
  namespace: default
  annotations:
    "gang-scheduling.preferred.jp/gang-name": "awesome_pfn"
    "gang-scheduling.preferred.jp/gang-size": "2"
    "gang-scheduling.preferred.jp/gang-schedule-timeout-seconds": "100"
spec:
  containers:
    - image: registry.k8s.io/pause:3.9
      name: pause
      ports:
        - containerPort: 80
      resources:
        limits:
          cpu: 100m
          memory: 500Mi
        requests:
          cpu: 100m
          memory: 500Mi
```

- `gang-scheduling.preferred.jp/gang-name`(`gangAnnotationPrefix`+`-name`): The name of gang which should be unique within the namespace.
- `gang-scheduling.preferred.jp/gang-size`(`gangAnnotationPrefix`+`-size`): The number of Pods belongs to this gang, 
that is, this Pod won't be scheduled successfully until this number of Pods is created and all can be scheduled to some Nodes.
- `gang-scheduling.preferred.jp/gang-schedule-timeout-seconds`(`gangAnnotationPrefix`+`-schedule-timeout-seconds`): This is the timeout configuration.
e.g., let's say one Pod is ready to get scheduled and waiting at [`WaitOnPermit`](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/#permit), while another Pod cannot be scheduled at the moment,
the waiting Pod is rejected once this timeout pass. 
Meaning, all the Pods in gang is put back to the queue and have to go through the scheduling cycle again.

### KubeSchedulerConfiguration

The cluster admin can configure gang plugin like following:

```yaml
kind: KubeSchedulerConfiguration
apiVersion: kubescheduler.config.k8s.io/v1
profiles:
  - schedulerName: awesome-pfn-scheduler
    plugins:
      multiPoint:
        enabled:
          - name: gang
    pluginConfig:
    - name: gang
      args:
        # GangAnnotationPrefix is the prefix of all gang annotations.
        # This configuration is required; if not set, the plugin will return an error during its initialization.
        gangAnnotationPrefix: "gang-scheduling.preferred.jp/gang"
        # SchedulerName is the name of the scheduler.
        # This field is optional; if not set, the default scheduler name will be used.
        schedulerName: "awesome-pfn-scheduler"
        # GangScheduleTimeoutSecondsLimit is the maximum timeout in seconds for gang scheduling.
        # If the timeout configured in the pod annotation exceeds this limit, the timeout will be set to this limit.
        # This field is optional; if not set, 100 will be used as a default value.
        gangScheduleTimeoutSecondsLimit: 100
        # GangScheduleDefaultTimeoutSeconds is the default timeout in seconds,
        # which will be used if the timeout is not set in the pod annotation.
        # This field is optional; if not set, 30 will be used as a default value.
        gangScheduleDefaultTimeoutSeconds: 30
        # GangScheduleTimeoutJitterSeconds is the jitter in seconds for timeout.
        # This field is optional; if not set, 30 will be used as a default value.
        gangScheduleTimeoutJitterSeconds: 30
```
