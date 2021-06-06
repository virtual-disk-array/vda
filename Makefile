OUT_DIR := ./_out
GOPATH := $(shell go env GOPATH)
PATH := $(PATH):$(GOPATH)/bin
VERSION := v0.0.2

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative pkg/proto/dataschema/dataschema.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/portalapi/portalapi.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/dnagentapi/dnagentapi.proto
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/cnagentapi/cnagentapi.proto

.PHONY: mock
mock:
	mockgen -destination=pkg/mocks/mockclient/mockclient.go -package=mockclient -source pkg/proto/portalapi/portalapi.pb.go
	mockgen -destination=pkg/mocks/mockcsi/mockutils.go -package=mockcsi -source pkg/csi/utils.go

.PHONY: compile
compile:
	mkdir -p $(OUT_DIR)/linux_amd64/
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_portal ./cmd/portal
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_monitor ./cmd/monitor
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_dn_agent ./cmd/dnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cn_agent ./cmd/cnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cli ./cmd/cli
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_admin ./cmd/admin
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_csi ./cmd/csi

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
	rm -rf pkg/mocks/mockclient/mockclient.go
	rm -rf pkg/mocks/mockcsi/mockutils.go
	go clean -testcache ./...

.PHONY: build
build: proto mock compile test

.DEFAULT_GOAL := build

.PHONY: image
image:
	docker build -t virtualdiskarray/vdacsi:$(VERSION) -f scripts/csi/Dockerfile .
	docker build -t virtualdiskarray/vdacsi:dev -f scripts/csi/Dockerfile .

.PHONY: push
push:
	docker push virtualdiskarray/vdacsi
