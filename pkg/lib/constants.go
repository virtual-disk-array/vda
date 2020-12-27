package lib

const (
	DefaultEtcdPrefix = "vda"

	PortalSucceedCode       = 0
	PortalSucceedMsg        = "succeed"
	PortalInternalErrCode   = 1
	PortalInternalErrMsg    = "internal error"
	PortalDupResErrCode     = 2
	PortalDupResErrMsg      = "duplicate resource"
	PortalUnknownResErrCode = 3
	PortalUnknownResErrMsg  = "unknown resource"

	DnSucceedCode   = 0
	DnSucceedMsg    = "succeed"
	DnOldRevErrCode = 1
	DnOldRevErrMsg  = "old revision"

	CnSucceedCode   = 0
	CnSucceedMsg    = "succeed"
	CnOldRevErrCode = 1
	CnOldRevErrMsg  = "old revision"

	ResUninitMsg = "uninit"
	ResNoInfoMsg = "no info"

	DefaultVdaPrefix = "vda"
	DefaultNqnPrefix = "nqn.2016-06.io.vda"
)
