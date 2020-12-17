package cnagent

import (
	"context"
	"fmt"
	"sync"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

var (
	mu      sync.Mutex
	lastReq *pbcn.SyncupCnRequest
)

func newErrInfo(err error) *pbcn.ErrInfo {
	if err != nil {
		return &pbcn.ErrInfo{
			IsErr:     true,
			ErrMsg:    err.Error(),
			Timestamp: lib.ResTimestamp(),
		}
	} else {
		return &pbcn.ErrInfo{
			IsErr:     false,
			ErrMsg:    "",
			Timestamp: lib.ResTimestamp(),
		}
	}
}

type syncupHelper struct {
	lisConf      *lib.LisConf
	nf           *lib.NameFmt
	oc           *lib.OperationClient
	feNvmeMap    map[string]bool
	aggBdevMap   map[string]bool
	daLvsMap     map[string]bool
	expNqnMap    map[string]bool
	secNvmeMap   map[string]bool
	grpBdevMap   map[string]bool
	raid0BdevMap map[string]bool
}

func (sh *syncupHelper) syncupPrimary(cntlrFeReq *pbcn.CntlrFeReq) *pbcn.CntlrFeRsp {
	var cntlrFeErr error
	var thisCntlr *pbcn.Controller
	secCntlrList := make([]*pbcn.Controller, 0)
	grpFeRspList := make([]*pbcn.GrpFeRsp, 0)
	snapFeRspList := make([]*pbcn.SnapFeRsp, 0)
	expFeRspList := make([]*pbcn.ExpFeRspList, 0)
	secNqnList := make([]string, 0)
	for _, cntlr := range cntlrFeReq.CntlrFeConf.CntlrList {
		if cntlrFeReq.CntlrId == cntlr.CntlrId {
			thisCntlr = cntlr
		} else {
			secCntlrList = append(secCntlrList, cntlr)
		}
	}
	if thisCntlr == nil {
		logger.Error("Can not find thisCntlr")
		cntlrFeErr = fmt.Errorf("Can not find thisCntlr")
	}

	bdevSeqList := make([]*lib.BdevSeq, 0)
	for _, grpFeReq := range cntlrFeReq {
		grpBdevName = sh.nf.GrpBdevName(grpFeReq.GrpId)
		bdevSeq := &lib.BdevSeq{
			Name: grpBdevName,
			Idx:  grpFeReq.GrpFeConf.GrpIdx,
		}
		bdevSeqList = append(bdevSeqList, bdevSeq)
		if cntlrFeErr == nil {
			grpFeRsp, cntlrFeErr := sh.syncupGrpFe(grpFeReq)
			grpFeRspList = append(grpFeRspList, grpFeRsp)
		}
	}

	aggBdevName := sh.nf.AggBdevName(cntlrFeReq.CntlrFeConf.DaId)
	sh.aggBdevMap[aggBdevName] = true
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.CreateAggBdev(aggBdevName, bdevSeqList)
	}
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.ExamineBdev(aggBdevName)
	}
	daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
	sh.daLvsMap[daLvsName] = true
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.CreateDaLvs(daLvsName, aggBdevName)
	}

	for _, snapFeReq := range cntlrFeReq.SnapFeReqList {
		if cntlrFeErr == nil {
			snapFeRsp, cntlrFeErr := sh.syncupSnapFe(snapFeReq)
			snapFeRspList = append(snapFeRspList, snapFeRsp)
		}
	}

	for cntlr := range secCntlrList {

	}
}

func (sh *syncupHelper) syncupSecondary(cntlrFeReq *pbcn.CntlrFeReq) *pbcn.CntlrFeRsp {
	return nil
}

func (sh *syncupHelper) syncupCntlrFe(cntlrFeReq *pbcn.CntlrFeReq) *pbcn.CntlrFeRsp {
	if cntlrFeReq.CntlrFeConf.IsPrimary {
		return syncupPrimary(cntrlFeReq)
	} else {
		return syncupSecondary(cntlrFeReq)
	}
}

func (sh *syncupHelper) syncupCn(cnReq *pbcn.CnReq) *pbcn.CnRsp {
	var cnErr error
	cntlrFeRspList := make([]*pbcn.CntlrFeRsp, 0)
	cnErr = sh.oc.LoadNvmfs()

	if cnErr == nil {
		for _, cntlrFeReq := range cnReq.CntlrFeReqList {
			cntlrFeRsp := sh.syncupCntlrFe(cntlrFeReq)
			cntlrFeRspList = append(cntlrFeRspList, cntlrFeRsp)
		}
	}

	return &pbcn.CnRsp{
		CnId: cnReq.CnId,
		CnInfo: &pbcn.CnInfo{
			ErrInfo: newErrInfo(cnErr),
		},
		CntlrFeRspList: cntlrFeRspList,
	}
}

func newSyncupHelper(lisConf *lib.LisConf, nf *lib.NameFmt, sc *lib.SpdkClient) *syncupHelper {
	return &syncupHelper{
		lisConf:      lisConf,
		nf:           nf,
		oc:           lib.NewOperationClient(sc),
		feNvmeMap:    make(map[string]bool),
		aggBdevMap:   make(map[string]bool),
		daLvsMap:     make(map[string]bool),
		expNqnMap:    make(map[string]bool),
		secNvmeMap:   make(map[string]bool),
		grpBdevMap:   make(map[string]bool),
		raid0BdevMap: make(map[string]bool),
	}
}

func (cnAgent *cnAgentServer) SyncupCn(ctx context.Context, req *pbcn.SyncupCnRequest) (
	*pbcn.SyncupCnReply, error) {
	mu.Lock()
	defer mu.Unlock()
	if lastReq != nil && req.Revision < lastReq.Revision {
		return &pbcn.SyncupCnReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOldRevErrCode,
				ReplyMsg:  lib.CnOldRevErrMsg,
			},
		}, nil
	}
	lastReq = req
	sh := newSyncupHelper(cnAgent.lisConf, cnAgent.nf, cnAgent.sc)

	cnRsp := sh.syncupCn(req.CnReq)
	return &pbcn.SyncupCnReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
		CnRsp: cnRsp,
	}, nil
}
