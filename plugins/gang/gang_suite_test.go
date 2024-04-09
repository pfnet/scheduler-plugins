package gang

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGang(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gang Tests")
}
