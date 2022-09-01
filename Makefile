OUT_DIR := ./_out
GOPATH := $(shell go env GOPATH)
PATH := $(PATH):$(GOPATH)/bin
VERSION := v0.2.0

.PHONY: cp_proto
cp_proto:
	protoc --go_out=. --go_opt=paths=source_relative pkg/proto/dataschema/dataschema.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/proto/portalapi/portalapi.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/proto/dnagentapi/dnagentapi.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/proto/cnagentapi/cnagentapi.proto

.PHONY: cp_mock
cp_mock:
	mockgen -destination=pkg/mocks/mockclient/mockclient.go -package=mockclient -source pkg/proto/portalapi/portalapi_grpc.pb.go
	mockgen -destination=pkg/mocks/mockcsi/mockutils.go -package=mockcsi -source pkg/csi/utils.go

.PHONY: cp_compile
cp_compile:
	mkdir -p $(OUT_DIR)/linux_amd64/
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_portal ./cmd/portal
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_monitor ./cmd/monitor
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_dn_agent ./cmd/dnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cn_agent ./cmd/cnagent
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_cli ./cmd/cli
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_admin ./cmd/admin
	env GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/linux_amd64/vda_csi ./cmd/csi

.PHONY: cp_test
cp_test:
	go test ./...

.PHONY: cp
cp: cp_proto cp_mock cp_compile cp_test

.PHONY: cpc
cpc:
	rm -rf $(OUT_DIR)/linux_amd64/vda_portal
	rm -rf $(OUT_DIR)/linux_amd64/vda_monitor
	rm -rf $(OUT_DIR)/linux_amd64/vda_dn_agent
	rm -rf $(OUT_DIR)/linux_amd64/vda_cn_agent
	rm -rf $(OUT_DIR)/linux_amd64/vda_cli
	rm -rf $(OUT_DIR)/linux_amd64/vda_admin
	rm -rf $(OUT_DIR)/linux_amd64/vda_csi
	rm -rf pkg/proto/dataschema/dataschema.pb.go
	rm -rf pkg/proto/portalapi/portalapi.pb.go
	rm -rf pkg/proto/dnagentapi/dnagentapi.pb.go
	rm -rf pkg/proto/cnagentapi/cnagentapi.pb.go
	rm -rf pkg/mocks/mockclient/mockclient.go
	rm -rf pkg/mocks/mockcsi/mockutils.go
	go clean -testcache ./...

.PHONY: dp_prepare
dp_prepare:
	bash scripts/dataplane/dataplane_prepare.sh

.PHONY: dp
dp:
	bash scripts/dataplane/dataplane_build.sh

.PHONY: dpc
dpc:
	bash scripts/dataplane/dataplane_clean.sh

.PHONY: dpc_all
dpc_all:
	bash scripts/dataplane/dataplane_clean.sh all

.PHONY: out_clean
out_clean:
	rm -rf _out

.PHONY: clean
clean: dpc cpc out_clean

.PHONY: build
build: dp cp

.DEFAULT_GOAL := build

.PHONY: image
image:
	docker build -t virtualdiskarray/vdacsi:$(VERSION) -f scripts/csi/Dockerfile .
	docker build -t virtualdiskarray/vdacsi:dev -f scripts/csi/Dockerfile .

.PHONY: push
push:
	docker push virtualdiskarray/vdacsi
