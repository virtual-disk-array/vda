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

	DnSucceedCode   = 1
	DnSucceedMsg    = "succeed"
	DnOldRevErrCode = 2

	CnSucceedCode   = 1
	CnSucceedMsg    = "succeed"
	CnOldRevErrCode = 2

	ResUninitMsg = "uninit"
	ResNoInfoMsg = "no info"

	DefaultVdaPrefix = "vda"
	DefaultNqnPrefix = "nqn.2016-06.io.vda"

	MaxHashCode = 0xffff

	AllocLockTTL  = 2
	AllocMaxRetry = 10

	DefaultSanpName        = "%default%"
	DefaultSanpDescription = "default snap"

	PortalDefaultListLimit = int64(100)
	PortalMaxListLimit     = int64(1000)
)
