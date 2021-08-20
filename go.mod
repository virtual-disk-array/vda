module github.com/virtual-disk-array/vda

go 1.15

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4
	github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible
	google.golang.org/api => google.golang.org/api v0.14.0
	google.golang.org/grpc => google.golang.org/grpc v1.27.0
)

require (
	github.com/container-storage-interface/spec v1.4.0
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.2
	github.com/kubernetes-csi/csi-lib-utils v0.9.1
	github.com/spf13/cobra v1.1.1
	google.golang.org/grpc v1.29.0
	google.golang.org/protobuf v1.24.0
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20210305010621-2afb4311ab10
)
