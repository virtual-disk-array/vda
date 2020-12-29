package cnagent

import (
	"context"
	"testing"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockspdk"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

const (
	freeCluster      = 100
	clusterSize      = 4 * 1024 * 1024
	totalDataCluster = 200
	blockSize        = 4096
	stripSizeKb      = uint32(64)
	grpSize          = uint64(10 * 1024 * 1024 * 1024)
	vdSizie          = uint64(10 * 1024 * 1024 * 1024)
	snapSize         = uint64(10 * 1024 * 1024 * 1024)
	reqId            = "4abf6ff7-46dd-48ec-8d51-276b304a30d1"
	cnId             = "e1951d01-e6d9-4659-bb62-47550ff4f0b2"
	daId             = "b742562a-0023-4018-a5ca-28a41abe92d4"
	grpId            = "892dbf9b-84cc-4c65-860d-4ee6f1d8982a"
	vdId             = "e29f755e-e32b-42d9-a820-7678b2b005b9"
	snapId           = "1ddb70f2-75f0-4708-b754-3a6b865a6335"
	expId            = "5c2ceaca-dddf-4511-81ed-56bebe383191"
	primCntlrId      = "339dc23b-f778-444b-a231-fa36da32e839"
	secCntlrId       = "47e4bc5c-e95a-4213-8021-768d1c0c9c42"
)

func mockNvmfGetSubsystems(params interface{}) (*mockspdk.SpdkErr, interface{}) {
	return nil, []map[string]interface{}{}
}

func mockBdevGetBdevs(params interface{}) (*mockspdk.SpdkErr, interface{}) {
	if params == nil {
		return nil, make([]map[string]interface{}, 0)
	} else {
		return &mockspdk.SpdkErr{
			Code:    -19,
			Message: "no such device",
		}, nil
	}
}

func mockBdevLvolGetLvstores(params interface{}) (*mockspdk.SpdkErr, interface{}) {
	return nil, []map[string]interface{}{
		map[string]interface{}{
			"uuid":               "xxx",
			"base_bdev":          "xxx",
			"free_clusters":      freeCluster,
			"cluster_size":       clusterSize,
			"total_data_cluster": totalDataCluster,
			"block_size":         blockSize,
			"name":               "xxx",
		},
	}
}

var (
	simpalSpdk = map[string]func(params interface{}) (*mockspdk.SpdkErr, interface{}){
		"nvmf_get_subsystems":    mockNvmfGetSubsystems,
		"bdev_get_bdevs":         mockBdevGetBdevs,
		"bdev_lvol_get_lvstores": mockBdevLvolGetLvstores,
	}
)

func TestPrimSyncup(t *testing.T) {
	sockPath := "/tmp/vdacntest.sock"
	sockTimeout := 10
	lisConf := &lib.LisConf{
		TrType:  "tcp",
		TrAddr:  "127.0.0.1",
		AdrFam:  "ipv4",
		TrSvcId: "4430",
	}
	trConf := map[string]interface{}{
		"trtype": "TCP",
	}
	s, err := mockspdk.NewMockSpdkServer("unix", sockPath, t)
	if err != nil {
		t.Errorf("Create mock spdk err: %v", err)
		return
	}
	defer s.Stop()
	for k, v := range simpalSpdk {
		s.AddMethod(k, v)
	}
	time.Sleep(time.Millisecond)
	cnAgent, err := newCnAgentServer(sockPath, sockTimeout, lisConf, trConf)
	if err != nil {
		t.Errorf("Create cn agent err: %v", err)
		return
	}
	defer cnAgent.Stop()
	ctx := context.Background()
	req := &pbcn.SyncupCnRequest{
		ReqId:    reqId,
		Revision: int64(1),
		CnReq: &pbcn.CnReq{
			CnId: cnId,
			CntlrFeReqList: []*pbcn.CntlrFeReq{
				&pbcn.CntlrFeReq{
					CntlrId: primCntlrId,
					CntlrFeConf: &pbcn.CntlrFeConf{
						DaId:        daId,
						StripSizeKb: stripSizeKb,
						CntlrList: []*pbcn.Controller{
							&pbcn.Controller{
								CntlrId:    primCntlrId,
								CnSockAddr: "xxx",
								CntlrIdx:   uint32(0),
								IsPrimary:  true,
								CnNvmfListener: &pbcn.NvmfListener{
									TrType:  "tcp",
									AdrFam:  "ipv4",
									TrAddr:  "127.0.0.1",
									TrSvcId: "4430",
								},
							},
							&pbcn.Controller{
								CntlrId:    secCntlrId,
								CnSockAddr: "xxx",
								CntlrIdx:   uint32(1),
								IsPrimary:  false,
								CnNvmfListener: &pbcn.NvmfListener{
									TrType:  "tcp",
									AdrFam:  "ipv4",
									TrAddr:  "127.0.0.1",
									TrSvcId: "4431",
								},
							},
						},
					},
					GrpFeReqList: []*pbcn.GrpFeReq{
						&pbcn.GrpFeReq{
							GrpId: grpId,
							GrpFeConf: &pbcn.GrpFeConf{
								GrpIdx: uint32(0),
								Size:   grpSize,
							},
							VdFeReqList: []*pbcn.VdFeReq{
								&pbcn.VdFeReq{
									VdId: vdId,
									VdFeConf: &pbcn.VdFeConf{
										DnNvmfListener: &pbcn.NvmfListener{
											TrType:  "tcp",
											AdrFam:  "ipv4",
											TrAddr:  "127.0.0.1",
											TrSvcId: "4420",
										},
										DnSockAddr: "xxx",
										VdIdx:      uint32(0),
										Size:       vdSizie,
									},
								},
							},
						},
					},
					SnapFeReqList: []*pbcn.SnapFeReq{
						&pbcn.SnapFeReq{
							SnapId: snapId,
							SnapFeConf: &pbcn.SnapFeConf{
								OriId:   "",
								IsClone: false,
								Idx:     uint64(0),
								Size:    snapSize,
							},
						},
					},
					ExpFeReqList: []*pbcn.ExpFeReq{
						&pbcn.ExpFeReq{
							ExpId: expId,
							ExpFeConf: &pbcn.ExpFeConf{
								InitiatorNqn: "xxx",
								SnapId:       snapId,
							},
						},
					},
				},
			},
		},
	}
	reply, err := cnAgent.SyncupCn(ctx, req)
	if err != nil {
		t.Errorf("SyncupCn err: %v", err)
		return
	}
	replyInfo := reply.ReplyInfo
	if replyInfo.ReplyCode != lib.CnSucceedCode {
		t.Errorf("SyncupCn ReplyCode wrong: %v", replyInfo.ReplyCode)
		return
	}
	if replyInfo.ReplyMsg != lib.CnSucceedMsg {
		t.Errorf("SyncupCn ReplyMsg wrong: %v", replyInfo.ReplyMsg)
		return
	}
	cnRsp := reply.CnRsp
	if cnRsp.CnId != cnId {
		t.Errorf("CnId mismatch: %v", cnRsp.CnId)
		return
	}
	if cnRsp.CnInfo.ErrInfo.IsErr {
		t.Errorf("CnRsp has err: %v", cnRsp.CnInfo.ErrInfo)
		return
	}
	if len(cnRsp.CntlrFeRspList) != 1 {
		t.Errorf("CntlrFeRspList len incorrect: %v", cnRsp.CntlrFeRspList)
		return
	}
	cntlrFeRsp := cnRsp.CntlrFeRspList[0]
	if cntlrFeRsp.CntlrId != primCntlrId {
		t.Errorf("cntlrId mismatch: %v", cntlrFeRsp)
		return
	}
	if cntlrFeRsp.CntlrFeInfo.ErrInfo.IsErr {
		t.Errorf("cntlrFeRsp has err: %v", cntlrFeRsp)
		return
	}
	if len(cntlrFeRsp.GrpFeRspList) != 1 {
		t.Errorf("GrpFeRspList len incorrect: %v", cntlrFeRsp.GrpFeRspList)
		return
	}
	grpFeRsp := cntlrFeRsp.GrpFeRspList[0]
	if grpFeRsp.GrpId != grpId {
		t.Errorf("grpId mismatch: %v", grpFeRsp)
		return
	}
	if grpFeRsp.GrpFeInfo.ErrInfo.IsErr {
		t.Errorf("grpFeRsp has err: %v", grpFeRsp)
		return
	}
	if len(grpFeRsp.VdFeRspList) != 1 {
		t.Errorf("VdFeRspList len incorrect: %v", grpFeRsp.VdFeRspList)
		return
	}
	vdFeRsp := grpFeRsp.VdFeRspList[0]
	if vdFeRsp.VdId != vdId {
		t.Errorf("vdId mismatch: %v", vdFeRsp)
		return
	}
	if vdFeRsp.VdFeInfo.ErrInfo.IsErr {
		t.Errorf("vdFeRsp has err: %v", vdFeRsp)
		return
	}
	if len(cntlrFeRsp.SnapFeRspList) != 1 {
		t.Errorf("SnapFeRspList len incorrect: %v", cntlrFeRsp.SnapFeRspList)
		return
	}
	snapFeRsp := cntlrFeRsp.SnapFeRspList[0]
	if snapFeRsp.SnapId != snapId {
		t.Errorf("snapId mismatch: %v", snapFeRsp)
		return
	}
	if snapFeRsp.SnapFeInfo.ErrInfo.IsErr {
		t.Errorf("snapFeRsp has err: %v", snapFeRsp)
		return
	}
	if len(cntlrFeRsp.ExpFeRspList) != 1 {
		t.Errorf("ExpFeRspList len incorrect: %v", cntlrFeRsp.ExpFeRspList)
		return
	}
	expFeRsp := cntlrFeRsp.ExpFeRspList[0]
	if expFeRsp.ExpId != expId {
		t.Errorf("expId mismatch: %v", expFeRsp)
		return
	}
	if expFeRsp.ExpFeInfo.ErrInfo.IsErr {
		t.Errorf("expFeRsp has err: %v", expFeRsp)
		return
	}
}

func TestSecSyncup(t *testing.T) {
	sockPath := "/tmp/vdacntest.sock"
	sockTimeout := 10
	lisConf := &lib.LisConf{
		TrType:  "tcp",
		TrAddr:  "127.0.0.1",
		AdrFam:  "ipv4",
		TrSvcId: "4430",
	}
	trConf := map[string]interface{}{
		"trtype": "TCP",
	}
	s, err := mockspdk.NewMockSpdkServer("unix", sockPath, t)
	if err != nil {
		t.Errorf("Create mock spdk err: %v", err)
		return
	}
	for k, v := range simpalSpdk {
		s.AddMethod(k, v)
	}
	time.Sleep(time.Millisecond)
	cnAgent, err := newCnAgentServer(sockPath, sockTimeout, lisConf, trConf)
	if err != nil {
		t.Errorf("Create cn agent err: %v", err)
		return
	}
	ctx := context.Background()
	req := &pbcn.SyncupCnRequest{
		ReqId:    reqId,
		Revision: int64(1),
		CnReq: &pbcn.CnReq{
			CnId: cnId,
			CntlrFeReqList: []*pbcn.CntlrFeReq{
				&pbcn.CntlrFeReq{
					CntlrId: secCntlrId,
					CntlrFeConf: &pbcn.CntlrFeConf{
						DaId:        daId,
						StripSizeKb: stripSizeKb,
						CntlrList: []*pbcn.Controller{
							&pbcn.Controller{
								CntlrId:    primCntlrId,
								CnSockAddr: "xxx",
								CntlrIdx:   uint32(0),
								IsPrimary:  true,
								CnNvmfListener: &pbcn.NvmfListener{
									TrType:  "tcp",
									AdrFam:  "ipv4",
									TrAddr:  "127.0.0.1",
									TrSvcId: "4430",
								},
							},
							&pbcn.Controller{
								CntlrId:    secCntlrId,
								CnSockAddr: "xxx",
								CntlrIdx:   uint32(1),
								IsPrimary:  false,
								CnNvmfListener: &pbcn.NvmfListener{
									TrType:  "tcp",
									AdrFam:  "ipv4",
									TrAddr:  "127.0.0.1",
									TrSvcId: "4431",
								},
							},
						},
					},
					ExpFeReqList: []*pbcn.ExpFeReq{
						&pbcn.ExpFeReq{
							ExpId: expId,
							ExpFeConf: &pbcn.ExpFeConf{
								InitiatorNqn: "xxx",
								SnapId:       snapId,
							},
						},
					},
				},
			},
		},
	}
	reply, err := cnAgent.SyncupCn(ctx, req)
	if err != nil {
		t.Errorf("SyncupCn err: %v", err)
		return
	}
	replyInfo := reply.ReplyInfo
	if replyInfo.ReplyCode != lib.CnSucceedCode {
		t.Errorf("SyncupCn ReplyCode wrong: %v", replyInfo.ReplyCode)
		return
	}
	if replyInfo.ReplyMsg != lib.CnSucceedMsg {
		t.Errorf("SyncupCn ReplyMsg wrong: %v", replyInfo.ReplyMsg)
		return
	}
	cnRsp := reply.CnRsp
	if cnRsp.CnId != cnId {
		t.Errorf("CnId mismatch: %v", cnRsp.CnId)
		return
	}
	if cnRsp.CnInfo.ErrInfo.IsErr {
		t.Errorf("CnRsp has err: %v", cnRsp.CnInfo.ErrInfo)
		return
	}
	if len(cnRsp.CntlrFeRspList) != 1 {
		t.Errorf("CntlrFeRspList len incorrect: %v", cnRsp.CntlrFeRspList)
		return
	}
	cntlrFeRsp := cnRsp.CntlrFeRspList[0]
	if cntlrFeRsp.CntlrId != secCntlrId {
		t.Errorf("cntlrId mismatch: %v", cntlrFeRsp)
		return
	}
	if cntlrFeRsp.CntlrFeInfo.ErrInfo.IsErr {
		t.Errorf("cntlrFeRsp has err: %v", cntlrFeRsp)
		return
	}
	expFeRsp := cntlrFeRsp.ExpFeRspList[0]
	if expFeRsp.ExpId != expId {
		t.Errorf("expId mismatch: %v", expFeRsp)
		return
	}
	if expFeRsp.ExpFeInfo.ErrInfo.IsErr {
		t.Errorf("expFeRsp has err: %v", expFeRsp)
		return
	}
}

func TestOldSyncup(t *testing.T) {
	sockPath := "/tmp/vdacntest.sock"
	sockTimeout := 10
	lisConf := &lib.LisConf{
		TrType:  "tcp",
		TrAddr:  "127.0.0.1",
		AdrFam:  "ipv4",
		TrSvcId: "4430",
	}
	trConf := map[string]interface{}{
		"trtype": "TCP",
	}
	s, err := mockspdk.NewMockSpdkServer("unix", sockPath, t)
	if err != nil {
		t.Errorf("Create mosk  spdk err: %v", err)
		return
	}
	defer s.Stop()
	for k, v := range simpalSpdk {
		s.AddMethod(k, v)
	}
	time.Sleep(time.Millisecond)

	cnAgent, err := newCnAgentServer(sockPath, sockTimeout, lisConf, trConf)
	if err != nil {
		t.Errorf("Create cn agent err: %v", err)
		return
	}
	defer cnAgent.Stop()

	ctx := context.Background()
	revOld := int64(1)
	revNew := int64(2)

	req := &pbcn.SyncupCnRequest{
		ReqId:    reqId,
		Revision: revNew,
		CnReq: &pbcn.CnReq{
			CnId:           cnId,
			CntlrFeReqList: []*pbcn.CntlrFeReq{},
		},
	}
	reply, err := cnAgent.SyncupCn(ctx, req)
	if err != nil {
		t.Errorf("SyncupCn err: %v", err)
	}
	replyInfo := reply.ReplyInfo
	if replyInfo.ReplyCode != lib.CnSucceedCode {
		t.Errorf("SyncupCn ReplyCode wrong: %v", replyInfo.ReplyCode)
		return
	}
	if replyInfo.ReplyMsg != lib.CnSucceedMsg {
		t.Errorf("SyncupCn ReplyMsg wrong: %v", replyInfo.ReplyMsg)
		return
	}

	req = &pbcn.SyncupCnRequest{
		ReqId:    reqId,
		Revision: revOld,
		CnReq: &pbcn.CnReq{
			CnId:           cnId,
			CntlrFeReqList: []*pbcn.CntlrFeReq{},
		},
	}
	reply, err = cnAgent.SyncupCn(ctx, req)
	if err != nil {
		t.Errorf("SyncupCn err: %v", err)
		return
	}
	replyInfo = reply.ReplyInfo
	if replyInfo.ReplyCode != lib.CnOldRevErrCode {
		t.Errorf("SyncupCn ReplyCode wrong: %v", replyInfo.ReplyCode)
		return
	}
}
