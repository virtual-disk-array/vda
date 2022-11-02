package lib

const (
	DefaultEtcdPrefix = "vda"

	PortalSucceedCode          = 0
	PortalSucceedMsg           = "succeed"
	PortalInternalErrCode      = 1
	PortalDupResErrCode        = 2
	PortalUnknownResErrCode    = 3
	PortalInvalidParamCode     = 4
	PortalResBusyErrCode       = 5
	PortalResRevErrCode        = 6
	PortalUnknownKeyTypeCode   = 7
	PortalJsonToMessageErrCode = 8
	PortalMessageToByteErrCode = 9

	DnSucceedCode      = 1
	DnSucceedMsg       = "succeed"
	DnOldRevErrCode    = 2
	DnOutOfSyncErrCode = 3

	CnSucceedCode      = 1
	CnSucceedMsg       = "succeed"
	CnOldRevErrCode    = 2
	CnOutOfSyncErrCode = 3

	ResUninitMsg = "uninit"
	ResNoInfoMsg = "no info"

	DefaultVdaPrefix = "vda"
	DefaultNqnPrefix = "nqn.2016-06.io.vda"

	MaxHashCode = 0xffff

	AllocLockTTL  = 2
	AllocMaxRetry = 10

	MainLvName = "main"

	PortalDefaultListLimit = int64(100)
	PortalMaxListLimit     = int64(1000)

	MonitorLeaseTimeout = 10

	GrpcCacheTTL      = 600
	GrpcCacheStep     = 10
	GrpcCacheInterval = 10

	DefaultStripCnt = 1
	DefaultStripSizeKb = 16
	DefaultClusterSize = uint64(4*1024*1024)
	DefaultExtendRatio = 10000
	DefaultInitGrpRatio = 10
	DefaultMaxGrpSize = uint64(100*1024*1024*1024)
	DefaultLowWaterMark = uint64(100*1024*1024)
)
