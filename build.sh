#!/bin/bash

export PATH="$PATH:$(go env GOPATH)/bin"
curr_dir=$(dirname $0)
cd $curr_dir
output_dir="_out"

function build_pb() {
    protoc --go_out=. --go_opt=paths=source_relative pkg/proto/dataschema/dataschema.proto
    protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/portalapi/portalapi.proto
    protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/dnagentapi/dnagentapi.proto
    protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative pkg/proto/cnagentapi/cnagentapi.proto
}

function clean_pb() {
    rm -f pkg/proto/portalapi/portalapi.pb.go
}

function build_portal() {
    go build -o $output_dir/vda_portal ./cmd/portal
}

function build_cli() {
    go build -o $output_dir/vda_cli ./cmd/cli
}

function build_dn_agent() {
    go build -o $output_dir/vda_dn_agent ./cmd/dnagent
}

function clean_portal() {
    rm -f $output_dir/vda_portal
}

# protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative portalapi/portalapi.proto

# mockgen -destination=pkg/mocks/mock_mounter.go -package=mocks k8s.io/utils/mount Interface
# mockgen -destination=pkg/mocks/mockmount/mock_mounter.go -package=mockmount k8s.io/utils/mount Interface
# mockgen -destination=pkg/mocks/mockmount/mock_execer.go -package=mockexec k8s.io/utils/exec Interface
# mockgen -destination=pkg/mocks/mockexec/mock_execer.go -package=mockexec k8s.io/utils/exec Interface
# mockgen -destination=pkg/mocks/mockcsidriver/mock_node_operator.go -package=mockcsidriver github.com/virtual-disk-array/vda/pkg/csidriver NodeOperatorInterface
# mockgen -destination=pkg/mocks/mockportalapi/mock_portal_client.go -package=mockportalapi github.com/virtual-disk-array/vda/pkg/proto/portalapi PortalClient
# go build -o vdacsi ./cmd/vdacsi

# docker build -t vda/vdacsi:v0.0.1 -f scripts/csi_dockerfile ./_out
# docker build -t vda/vdacsi:v0.0.1 -f Dockerfile .

action=$1

case $action in
    build)
        build_pb
        build_portal
        build_dn_agent
        build_cli
        ;;
    clean)
        clean_grpc
        clean_portal
        ;;
    *)
        echo "unknow action: $action"
        exit 1
        ;;
esac
