export GO111MODULE=on
export CGO_ENABLED=0

REVISION			:= $(shell git rev-parse --short HEAD)$(shell [ -z "$$(git status --short)" ] || echo "-dirty")
SCHEDULER_VERSION	?= $(shell cat ./VERSION)
SCHEDULER_IMAGE_TAG ?= $(SCHEDULER_VERSION)
SCHEDULER_LDFLAGS	:= -ldflags="-s -w -X \"main.Version=$(SCHEDULER_VERSION)\" -X \"main.Revision=$(REVISION)\" -extldflags \"-static\""
