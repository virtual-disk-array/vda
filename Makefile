OUT_DIR := ./_out
GOPATH := $(shell go env GOPATH)
PATH := $(PATH):$(GOPATH)/bin

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative pkg/proto/dataschema/dataschema.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/portalapi/portalapi.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/dnagentapi/dnagentapi.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/cnagentapi/cnagentapi.proto

.PHONY: compile
compile:
	mkdir -p $(OUT_DIR)/linux_amd64/
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_portal ./cmd/portal
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_monitor ./cmd/monitor
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_dn_agent ./cmd/dnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cn_agent ./cmd/cnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cli ./cmd/cli
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_admin ./cmd/admin

.PHONY: test
test:
	go test ./...

.PHONY: clean
clean:
	rm -rf $(OUT_DIR)
	rm -rf pkg/proto/dataschema/dataschema.pb.go
	rm -rf pkg/proto/portalapi/portalapi.pb.go
	rm -rf pkg/proto/dnagentapi/dnagentapi.pb.go
	rm -rf pkg/proto/cnagentapi/cnagentapi.pb.go
	go clean -testcache ./...

.PHONY: all
all: proto compile test

.DEFAULT_GOAL := all
