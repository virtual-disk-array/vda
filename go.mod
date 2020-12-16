module github.com/virtual-disk-array/vda

go 1.15

replace (
	google.golang.org/api => google.golang.org/api v0.14.0
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)

require (
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.2
	github.com/spf13/cobra v1.1.1
	google.golang.org/grpc v1.23.0
)
