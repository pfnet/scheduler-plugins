include	include.mk

OUT_DIR ?= bin

# Build scheduler binary
.PHONY: build
build:
	go build                                                  \
		-tags netgo -installsuffix netgo $(SCHEDULER_LDFLAGS) \
		-o $(OUT_DIR)/scheduler \
		cmd/kube-scheduler/scheduler.go

# Run unit tests
.PHONY: test
test:
	go test -v ./...

.PHONY: lint
lint:
	if which docker > /dev/null; then \
		docker run --rm \
			-v "./:/go/src/github.com/preferred-ext/scheduler-plugins" \
			-w /go/src/github.com/preferred-ext/scheduler-plugins\
			golangci/golangci-lint:v1.55.2 golangci-lint run -v; \
	else \
		golangci-lint run -v; \
	fi

.PHONY: clean
clean:
	rm -rf $(OUT_DIR)

