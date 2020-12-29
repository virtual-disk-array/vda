package lib

type DnPdCand struct {
	SockAddr string
	PdName   string
}

type CnCand struct {
	SockAddr string
}

type BdevQos struct {
	RwIosPerSec    uint64
	RwMbytesPerSec uint64
	RMbytesPerSec  uint64
	WMbytesPerSec  uint64
}

func AllocateDnPd(vdCnt uint32, vdSize uint64, qos *BdevQos) (
	[]*DnPdCand, error) {
	return nil, nil
}

func AllocateCn(cnCnt uint32) ([]*CnCand, error) {
	return nil, nil
}
