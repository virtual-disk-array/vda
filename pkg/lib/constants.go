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
	DnTryLockErrCode   = 3

	CnSucceedCode      = 1
	CnSucceedMsg       = "succeed"
	CnOldRevErrCode    = 2
	CnTryLockErrCode   = 3

	ResUninitMsg = "uninit"
	ResNoInfoMsg = "no info"
	FailoverMsg  = "failover"

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

	DefaultCntlrCnt = 1
	DefaultStripSizeKb = 16
	DefaultRaid0BdevCnt = 1
	DefaultClusterSize = 4*1024*1024
	DefaultExtendRatio = 10000
	DefaultInitGrpRatio = 10
	DefaultMaxGrpSize = uint64(100*1024*1024*1024)
	DefaultLowWaterMark = uint64(100*1024*1024)
	DefaultRwIosPerSec = 0
	DefaultRwMbytesPerSec = 0
	DefaultRMbytesPerSec = 0
	DefaultWMbytesPerSec = 0
	DefaultBitSizeKb = 4*1024*1024
	DefaultKeepSeconds = uint64(3600*24)

	TaskStatusProcessing = "processing"
	TaskStatusCanceled   = "canceled"
	TaskStatusCompleted  = "completed"
	TaskStatusFailed     = "failed"

	SusresStatusResumed    = "resumed"
	SusresStatusResuming   = "resuming"
	SusresStatusSuspended  = "suspended"
	SusresStatusSuspending = "suspending"

	BdevProductRaid1  = "raid1"
	BdevProductSusres = "susres"

	NullBdevBlockSize = uint64(4096)
	NullBdevNumBlocks = uint64(16384)
	Raid1MetaSize = NullBdevBlockSize * NullBdevNumBlocks
	ConcatStripSizeKb = uint32(4)

	RaidLevelConcat = "concat"

	SingleHealthyValNone = "None"
	SingleHealhtyValLeg0 = "Leg0"
	SingleHealhtyValLeg1 = "Leg1"
	SingleHealhtyActNone = "SetNone"
	SingleHealthyActLeg0 = "SetLeg0"
	SingleHealthyActLeg1 = "SetLeg1"
	SingleHealthyActNoChange = "NoChange"
)
