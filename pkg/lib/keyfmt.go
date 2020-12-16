package lib

import (
	"fmt"
)

type KeyFmt struct {
	prefix string
}

func (kf *KeyFmt) DnEntityKey(sockAddr string) string {
	return fmt.Sprintf("/%s/dn/%s", kf.prefix, sockAddr)
}

func (kf *KeyFmt) CnEntityKey(sockAddr string) string {
	return fmt.Sprintf("/%s/cn/%s", kf.prefix, sockAddr)
}

func (kf *KeyFmt) DaEntityKey(daName string) string {
	return fmt.Sprintf("/%s/da/%s", kf.prefix, daName)
}

func (kf *KeyFmt) DnCapKey(freeSize uint64, sockAddr string, pdName string) string {
	return fmt.Sprintf("/%s/capacity/dn/%d@%s@%s",
		kf.prefix, freeSize, sockAddr, pdName)
}

func (kf *KeyFmt) CnCapKey(cntlrCnt uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/capacity/cn/%d@%s",
		kf.prefix, cntlrCnt, sockAddr)
}

func (kf *KeyFmt) DnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/list/dn/%d@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) CnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/list/cn/%d@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) DaListKey(daName string) string {
	return fmt.Sprintf("/%s/list/da/%s", kf.prefix, daName)
}

func (kf *KeyFmt) DnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/error/dn/%d@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) CnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/error/cn/%d@%s", kf.prefix, hashCode, sockAddr)
}

func NewKeyFmt(prefix string) *KeyFmt {
	return &KeyFmt{
		prefix: prefix,
	}
}
