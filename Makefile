# The targets every t10 sub project has (the workspace's proposals/dev-loop.md).
# CI runs make ci.
# make test needs nothing running.

GO_FILES = $(shell git ls-files '*.go' | grep -v '^third_party/')

.PHONY: build lint test ci

build:
	go build ./...

# gofmt comes from the toolchain go.mod pins, which go switches to; the gofmt on
# PATH may be another release, and they don't always agree.
lint:
	go vet ./...
	@out=$$("$$(go env GOROOT)/bin/gofmt" -l $(GO_FILES)); [ -z "$$out" ] || { echo "gofmt -w these:"; echo "$$out"; exit 1; }

# Database tests skip.
test:
	TEST_DATABASE_URL= go test ./...

ci: lint build test
