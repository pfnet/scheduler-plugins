package utils

import (
	"github.com/ghodss/yaml"
	"k8s.io/apimachinery/pkg/runtime"
)

func YamlToConfig(configYaml string) *runtime.Unknown {
	jsonRaw, err := yaml.YAMLToJSON([]byte(configYaml))
	if err != nil {
		// This is the test code, so we can just panic here if something is wrong.
		panic(err)
	}
	return &runtime.Unknown{Raw: jsonRaw}
}
