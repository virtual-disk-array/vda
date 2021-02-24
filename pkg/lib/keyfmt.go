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

type InvalidKeyError struct {
	msg string
}

func (e *InvalidKeyError) Error() string {
	return e.msg
}

type KeyFmt struct {
	prefix string
}

func (kf *KeyFmt) DnEntityPrefix() string {
	return fmt.Sprintf("/%s/dn/", kf.prefix)
}
func (kf *KeyFmt) DnEntityKey(sockAddr string) string {
	return fmt.Sprintf("%s%s", kf.DnEntityPrefix(), sockAddr)
}

func (kf *KeyFmt) CnEntityPrefix() string {
	return fmt.Sprintf("/%s/cn/", kf.prefix)
}

func (kf *KeyFmt) CnEntityKey(sockAddr string) string {
	return fmt.Sprintf("%s%s", kf.CnEntityPrefix(), sockAddr)
}

func (kf *KeyFmt) DaEntityPrefix() string {
	return fmt.Sprintf("/%s/da/", kf.prefix)
}

func (kf *KeyFmt) DaEntityKey(daName string) string {
	return fmt.Sprintf("%s%s", kf.DaEntityPrefix(), daName)
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
		msg := fmt.Sprintf("Invalid DnCapKey: %s", key)
		return uint64(0), "", "", &InvalidKeyError{msg}
	}
	items := strings.Split(key[len(capPrefix):], "@")
	if len(items) != 3 {
		return uint64(0), "", "", fmt.Errorf(
			"DnCapKey less than 3 items: %s", key)
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
		msg := fmt.Sprintf("Invalid CnKapKey: %s", key)
		return uint32(0), "", &InvalidKeyError{msg}
	}
	items := strings.Split(key[len(capPrefix):], "@")
	if len(items) != 2 {
		return uint32(0), "", fmt.Errorf(
			"CnCapKey less than 2 items: %s", key)
	}
	cnt, err := strconv.ParseUint(items[0], 16, 32)
	if err != nil {
		return uint32(0), "", err
	}
	cntlrCnt := maxUint32 - uint32(cnt)
	return cntlrCnt, items[1], nil
}

func (kf *KeyFmt) DnListPrefix() string {
	return fmt.Sprintf("/%s/list/dn/", kf.prefix)
}

func (kf *KeyFmt) DnListWithHash(hashCode uint32) string {
	return fmt.Sprintf("%s%08x", kf.DnListPrefix(), hashCode)
}

func (kf *KeyFmt) DnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("%s@%s", kf.DnListWithHash(hashCode), sockAddr)
}

func (kf *KeyFmt) DecodeDnListKey(key string) (uint32, string, error) {
	prefix := kf.DnListPrefix()
	if !strings.HasPrefix(key, prefix) {
		msg := fmt.Sprintf("Invalid DnListKey: %s", key)
		return uint32(0), "", &InvalidKeyError{msg}
	}
	items := strings.Split(key[len(prefix):], "@")
	if len(items) != 2 {
		return uint32(0), "", fmt.Errorf("DnListKey less than 2")
	}
	hashCode, err := strconv.ParseUint(items[0], 16, 32)
	if err != nil {
		return uint32(0), "", err
	}
	return uint32(hashCode), items[1], nil
}

func (kf *KeyFmt) CnListPrefix() string {
	return fmt.Sprintf("/%s/list/cn/", kf.prefix)
}

func (kf *KeyFmt) CnListKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("%s%08x@%s", kf.CnListPrefix(), hashCode, sockAddr)
}

func (kf *KeyFmt) DecodeCnListKey(key string) (uint32, string, error) {
	prefix := kf.CnListPrefix()
	if !strings.HasPrefix(key, prefix) {
		msg := fmt.Sprintf("Invalid CnListKey: %s", key)
		return uint32(0), "", &InvalidKeyError{msg}
	}
	items := strings.Split(key[len(prefix):], "@")
	if len(items) != 2 {
		return uint32(0), "", fmt.Errorf("CnListKey less than 2")
	}
	hashCode, err := strconv.ParseUint(items[0], 16, 32)
	if err != nil {
		return uint32(0), "", err
	}
	return uint32(hashCode), items[1], nil
}

func (kf *KeyFmt) DaListPrefix() string {
	return fmt.Sprintf("/%s/list/da/", kf.prefix)
}

func (kf *KeyFmt) DaListKey(daName string) string {
	return fmt.Sprintf("%s%s", kf.DaListPrefix(), daName)
}

func (kf *KeyFmt) DecodeDaListKey(key string) (string, error) {
	prefix := kf.DaListPrefix()
	if !strings.HasPrefix(key, prefix) {
		msg := fmt.Sprintf("Invalid DaListKey: %s", key)
		return "", &InvalidKeyError{msg}
	}
	return key[len(prefix):], nil
}

func (kf *KeyFmt) DnErrPrefix() string {
	return fmt.Sprintf("/%s/error/dn/", kf.prefix)
}

func (kf *KeyFmt) DnErrWithHash(hashCode uint32) string {
	return fmt.Sprintf("%s%08x", kf.DnErrPrefix(), hashCode)
}

func (kf *KeyFmt) DnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("%s@%s", kf.DnErrWithHash(hashCode), sockAddr)
}

func (kf *KeyFmt) DecodeDnErrKey(key string) (uint32, string, error) {
	prefix := kf.DnErrPrefix()
	if !strings.HasPrefix(key, prefix) {
		msg := fmt.Sprintf("Invalid DnErrKey: %s", key)
		return uint32(0), "", &InvalidKeyError{msg}
	}
	items := strings.Split(key[len(prefix):], "@")
	if len(items) != 2 {
		return uint32(0), "", fmt.Errorf("DnErrKey less than 2")
	}
	hashCode, err := strconv.ParseUint(items[0], 16, 32)
	if err != nil {
		return uint32(0), "", err
	}
	return uint32(hashCode), items[1], nil
}

func (kf *KeyFmt) CnErrPrefix() string {
	return fmt.Sprintf("/%s/error/cn/", kf.prefix)
}

func (kf *KeyFmt) CnErrKey(hashCode uint32, sockAddr string) string {
	return fmt.Sprintf("%s%08x@%s", kf.CnErrPrefix(), hashCode, sockAddr)
}

func (kf *KeyFmt) AllocLockPath() string {
	return fmt.Sprintf("%s/lock/alloc", kf.prefix)
}

func (kf *KeyFmt) MonitorPrefix() string {
	return fmt.Sprintf("%s/monitor", kf.prefix)
}

func NewKeyFmt(prefix string) *KeyFmt {
	return &KeyFmt{
		prefix: prefix,
	}
}
