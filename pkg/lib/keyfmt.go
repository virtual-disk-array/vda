package lib

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	maxUint64 = uint64(0xffffffffffffffff)
	maxUint32 = uint32(0xffffffff)
)

type OutOfRangeError struct {
	msg string
}

func (e *OutOfRangeError) Error() string {
	return e.msg
}

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

func (kf *KeyFmt) DnCapPrefix() string {
	return fmt.Sprintf("/%s/capacity/dn/", kf.prefix)
}

func (kf *KeyFmt) DnCapSizePrefix(freeSize uint64) string {
	return fmt.Sprintf("%s%016x", kf.DnCapPrefix(), maxUint64-freeSize)
}

func (kf *KeyFmt) DnCapKey(freeSize uint64, sockAddr string, pdName string) string {
	return fmt.Sprintf("%s@%s@%s", kf.DnCapSizePrefix(freeSize), sockAddr, pdName)
}

func (kf *KeyFmt) DecodeDnCapKey(key string) (
	uint64, string, string, error) {
	capPrefix := kf.DnCapPrefix()
	if !strings.HasPrefix(key, capPrefix) {
		msg := fmt.Sprintf("DnCap key out of range: %s", key)
		return uint64(0), "", "", &OutOfRangeError{msg}
	}
	items := strings.Split(key[len(capPrefix):], "@")
	if len(items) != 3 {
		return uint64(0), "", "", fmt.Errorf(
			"DnCap key less than 3 items: %s", key)
	}
	size, err := strconv.ParseUint(items[0], 16, 64)
	if err != nil {
		return uint64(0), "", "", err
	}
	freeSize := maxUint64 - size
	return freeSize, items[1], items[2], nil
}

func (kf *KeyFmt) CnCapPrefix() string {
	return fmt.Sprintf("/%s/capacity/cn/", kf.prefix)
}

func (kf *KeyFmt) CnCapCntPrefix(cntlrCnt uint32) string {
	return fmt.Sprintf("%s%08x", kf.CnCapPrefix(), maxUint32-cntlrCnt)
}

func (kf *KeyFmt) CnCapKey(cntlrCnt uint32, sockAddr string) string {
	return fmt.Sprintf("%s@%s", kf.CnCapCntPrefix(cntlrCnt), sockAddr)
}

func (kf *KeyFmt) DecodeCnCapKey(key string) (
	uint32, string, error) {
	capPrefix := kf.CnCapPrefix()
	if !strings.HasPrefix(key, capPrefix) {
		msg := fmt.Sprintf("CnCap key out of range: %s", key)
		return uint32(0), "", &OutOfRangeError{msg}
	}
	items := strings.Split(key[len(capPrefix):], "@")
	if len(items) != 2 {
		return uint32(0), "", fmt.Errorf(
			"CnCap key less than 2 items: %s", key)
	}
	cnt, err := strconv.ParseUint(items[0], 16, 32)
	if err != nil {
		return uint32(0), "", err
	}
	cntlrCnt := maxUint32 - uint32(cnt)
	return cntlrCnt, items[1], nil
}

func (kf *KeyFmt) DnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/list/dn/%04x@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) CnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/list/cn/%04x@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) DaListKey(daName string) string {
	return fmt.Sprintf("/%s/list/da/%s", kf.prefix, daName)
}

func (kf *KeyFmt) DnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/error/dn/%04x@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) CnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("/%s/error/cn/%04x@%s", kf.prefix, hashCode, sockAddr)
}

func (kf *KeyFmt) AllocLockPath() string {
	return fmt.Sprintf("%s/lock/alloc", kf.prefix)
}

func NewKeyFmt(prefix string) *KeyFmt {
	return &KeyFmt{
		prefix: prefix,
	}
}
