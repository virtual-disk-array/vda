package lib

const (
	DefaultEtcdPrefix = "vda"

	PortalSucceedCode       = 1
	PortalSucceedMsg        = "succeed"
	PortalInternalErrCode   = 2
	PortalDupResErrCode     = 3
	PortalUnknownResErrCode = 4
	PortalInvalidParamCode  = 5

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

	MaxHashCode = 65536

	AllocLockTTL  = 2
	AllocMaxRetry = 10

	DefaultSanpName        = "%default%"
	DefaultSanpDescription = "default snap"
)
