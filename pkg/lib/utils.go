package lib

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ctxKey string

func (c ctxKey) String() string {
	return "ctx key " + string(c)
}

var (
	ctxKeyReqId = ctxKey("reqId")
)

func SetReqId(ctx context.Context) context.Context {
	reqId := uuid.New().String()
	newCtx := context.WithValue(ctx, ctxKeyReqId, reqId)
	return newCtx
}

func GetReqId(ctx context.Context) string {
	reqId, ok := ctx.Value(ctxKeyReqId).(string)
	if ok {
		return reqId
	} else {
		return "???"
	}
}

func NewHexStrUuid() string {
	id := uuid.New()
	return fmt.Sprintf("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
		id[8], id[9], id[10], id[11], id[12], id[13], id[14], id[15])
}

func ResTimestamp() string {
	return time.Now().UTC().String()
}

func NvmfSerailNumber(nqn string) string {
	hash := md5.Sum([]byte(nqn))
	return hex.EncodeToString(hash[:])[:20]
}

func NvmfUuid(nqn string) (string, error) {
	hash := md5.Sum([]byte(nqn))
	id, err := uuid.FromBytes(hash[:])
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
