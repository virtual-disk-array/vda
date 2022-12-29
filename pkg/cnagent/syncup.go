package cnagent

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
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
	vdSusresMap  map[string]bool
	mtNullMap    map[string]bool
	mtConcatMap  map[string]bool
	mtRaid1Map   map[string]bool
	aggBdevMap   map[string]bool
	daLvsMap     map[string]bool
	snapMap      map[string]bool
	expNqnMap    map[string]bool
	grpBdevMap   map[string]bool
	raid0BdevMap map[string]bool
	raid1BdevMap map[string]bool
	secNvmeMap   map[string]bool
	nullBdevMap  map[string]bool
	idxToMt      map[string]*pbcn.MtFeReq
	mtFeRspList  []*pbcn.MtFeRsp
	reqId        string
}

type bdevSeq struct {
	Name string
	Idx  uint32
}

func mtKey(grpIdx uint32, vdIdx uint32) string {
	return fmt.Sprintf("%d-%d", grpIdx, vdIdx)
}

func (sh *syncupHelper) syncupVdFeNormal(cntlrFeReq *pbcn.CntlrFeReq,
	grpFeReq *pbcn.GrpFeReq, vdFeReq *pbcn.VdFeReq) *pbcn.VdFeRsp {
	var vdFeErr error
	beNqnName := sh.nf.BeNqnName(vdFeReq.VdId)
	feNvmeName := sh.nf.FeNvmeName(vdFeReq.VdId)
	sh.feNvmeMap[feNvmeName] = true
	feNqnName := sh.nf.FeNqnName(cntlrFeReq.CntlrId)
	lisConf := &lib.LisConf{
		TrType:  vdFeReq.VdFeConf.DnNvmfListener.TrType,
		AdrFam:  vdFeReq.VdFeConf.DnNvmfListener.AdrFam,
		TrAddr:  vdFeReq.VdFeConf.DnNvmfListener.TrAddr,
		TrSvcId: vdFeReq.VdFeConf.DnNvmfListener.TrSvcId,
	}

	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateFeNvme(
			feNvmeName, beNqnName, feNqnName, lisConf)
	}

	feBdevName := sh.nf.FeBdevName(vdFeReq.VdId)
	vdSusresName := sh.nf.VdSusresName(cntlrFeReq.CntlrFeConf.DaId,
		grpFeReq.GrpFeConf.GrpIdx, vdFeReq.VdFeConf.VdIdx)
	sh.vdSusresMap[vdSusresName] = true
	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateNormalSusres(vdSusresName, feBdevName)
	}
	vdFeRsp := &pbcn.VdFeRsp{
		VdId: vdFeReq.VdId,
		VdFeInfo: &pbcn.VdFeInfo{
			ErrInfo: newErrInfo(vdFeErr),
		},
	}
	return vdFeRsp
}

func (sh *syncupHelper) syncupVdFeMt(cntlrFeReq *pbcn.CntlrFeReq,
	mtFeReq *pbcn.MtFeReq) *pbcn.VdFeRsp {
	var vdFeErr error
	feNqnName := sh.nf.FeNqnName(cntlrFeReq.CntlrId)

	srcBeNqnName := sh.nf.BeNqnName(mtFeReq.MtFeConf.SrcVdId)
	srcFeNvmeName := sh.nf.FeNvmeName(mtFeReq.MtFeConf.SrcVdId)
	sh.feNvmeMap[srcFeNvmeName] = true
	srcLisConf := &lib.LisConf{
		TrType:  mtFeReq.MtFeConf.SrcListener.TrType,
		AdrFam:  mtFeReq.MtFeConf.SrcListener.AdrFam,
		TrAddr:  mtFeReq.MtFeConf.SrcListener.TrAddr,
		TrSvcId: mtFeReq.MtFeConf.SrcListener.TrSvcId,
	}

	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateFeNvme(
			srcFeNvmeName, srcBeNqnName, feNqnName, srcLisConf)
	}

	vdFeRsp := &pbcn.VdFeRsp{
		VdId: mtFeReq.MtFeConf.SrcVdId,
		VdFeInfo: &pbcn.VdFeInfo{
			ErrInfo: newErrInfo(vdFeErr),
		},
	}

	dstBeNqnName := sh.nf.BeNqnName(mtFeReq.MtFeConf.DstVdId)
	dstFeNvmeName := sh.nf.FeNvmeName(mtFeReq.MtFeConf.DstVdId)
	sh.feNvmeMap[dstFeNvmeName] = true
	dstLisConf := &lib.LisConf{
		TrType:  mtFeReq.MtFeConf.DstListener.TrType,
		AdrFam:  mtFeReq.MtFeConf.DstListener.AdrFam,
		TrAddr:  mtFeReq.MtFeConf.DstListener.TrAddr,
		TrSvcId: mtFeReq.MtFeConf.DstListener.TrSvcId,
	}

	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateFeNvme(
			dstFeNvmeName, dstBeNqnName, feNqnName, dstLisConf)
	}

	srcFeBdevName := sh.nf.FeBdevName(mtFeReq.MtFeConf.SrcVdId)
	dstFeBdevName := sh.nf.FeBdevName(mtFeReq.MtFeConf.DstVdId)
	srcMtNullName := sh.nf.MtNullName(mtFeReq.MtFeConf.SrcVdId)
	sh.mtNullMap[srcMtNullName] = true
	dstMtNullName := sh.nf.MtNullName(mtFeReq.MtFeConf.DstVdId)
	sh.mtNullMap[dstMtNullName] = true
	srcMtConcatName := sh.nf.MtConcatName(mtFeReq.MtFeConf.SrcVdId)
	sh.mtConcatMap[srcMtConcatName] = true
	dstMtConcatName := sh.nf.MtConcatName(mtFeReq.MtFeConf.DstVdId)
	sh.mtConcatMap[dstMtConcatName] = true
	mtRaid1Name := sh.nf.MtRaid1Name(mtFeReq.MtId)
	sh.mtRaid1Map[mtRaid1Name] = true
	vdSusresName := sh.nf.VdSusresName(cntlrFeReq.CntlrFeConf.DaId,
		mtFeReq.MtFeConf.GrpIdx, mtFeReq.MtFeConf.VdIdx)
	sh.vdSusresMap[vdSusresName] = true
	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateMtSusres(vdSusresName, mtRaid1Name,
			mtFeReq.MtFeConf.Raid1Conf.BitSizeKb,
			srcMtConcatName, dstMtConcatName,
			srcMtNullName, dstMtNullName,
			srcFeBdevName, dstFeBdevName)
	}

	mtFeRsp := &pbcn.MtFeRsp{
		MtId: mtFeReq.MtId,
		MtFeInfo: &pbcn.MtFeInfo{
			ErrInfo: newErrInfo(vdFeErr),
		},
	}
	if vdFeErr == nil {
		raid1Info, vdFeErr := sh.oc.GetRaid1Info(mtRaid1Name)
		if vdFeErr == nil {
			mtFeRsp.MtFeInfo.Raid1Info = &pbcn.Raid1Info{
				Bdev0Online: raid1Info.Bdev0Online,
				Bdev1Online: raid1Info.Bdev1Online,
				TotalBit:    raid1Info.TotalBit,
				SyncedBit:   raid1Info.SyncedBit,
				ResyncIoCnt: raid1Info.ResyncIoCnt,
				Status:      raid1Info.Status,
			}
		}
	}
	sh.mtFeRspList = append(sh.mtFeRspList, mtFeRsp)
	return vdFeRsp
}

func (sh *syncupHelper) syncupVdFe(cntlrFeReq *pbcn.CntlrFeReq,
	grpFeReq *pbcn.GrpFeReq, vdFeReq *pbcn.VdFeReq) *pbcn.VdFeRsp {
	logger.Info("syncupVdFe: %v", vdFeReq)
	key := mtKey(grpFeReq.GrpFeConf.GrpIdx, vdFeReq.VdFeConf.VdIdx)
	mtFeReq, ok := sh.idxToMt[key]

	if ok {
		return sh.syncupVdFeMt(cntlrFeReq, mtFeReq)
	} else {
		return sh.syncupVdFeNormal(cntlrFeReq, grpFeReq, vdFeReq)
	}
}

func (sh *syncupHelper) syncupGrpFe(cntlrFeReq *pbcn.CntlrFeReq,
	grpFeReq *pbcn.GrpFeReq) *pbcn.GrpFeRsp {
	logger.Info("syncupGrpFe: %v", grpFeReq)
	var grpFeErr error
	vdFeRspList := make([]*pbcn.VdFeRsp, 0)
	vdFeReqList := grpFeReq.VdFeReqList
	sort.Slice(vdFeReqList, func(i, j int) bool {
		return vdFeReqList[i].VdFeConf.VdIdx > vdFeReqList[j].VdFeConf.VdIdx
	})
	vdSusresList := make([]string, 0)
	for _, vdFeReq := range vdFeReqList {
		vdSusresName := sh.nf.VdSusresName(cntlrFeReq.CntlrFeConf.DaId,
			grpFeReq.GrpFeConf.GrpIdx, vdFeReq.VdFeConf.VdIdx)
		vdSusresList = append(vdSusresList, vdSusresName)
		vdFeRsp := sh.syncupVdFe(cntlrFeReq, grpFeReq, vdFeReq)
		vdFeRspList = append(vdFeRspList, vdFeRsp)
		if vdFeRsp.VdFeInfo.ErrInfo.IsErr {
			grpFeErr = fmt.Errorf("VdFeError")
		}
	}

	raid0BdevList := make([]string, 0)
	if cntlrFeReq.CntlrFeConf.Redundancy == nil {
		for _, bdevName := range vdSusresList {
			raid0BdevList = append(raid0BdevList, bdevName)
		}
	} else {
		switch x := cntlrFeReq.CntlrFeConf.Redundancy.(type) {
		case *pbcn.CntlrFeConf_Raid1Conf:
			var leg0, leg1 string
			for i, bdevName := range vdSusresList {
				if i % 2 == 0 {
					leg0 = bdevName
				} else {
					leg1 = bdevName
					raid1BdevName := sh.nf.Raid1BdevName(
						leg0, leg1)
					sh.raid1BdevMap[raid1BdevName] = true
					raid0BdevList = append(
						raid0BdevList, raid1BdevName)
					if grpFeErr != nil {
						grpFeErr = sh.oc.CreateRaid1Bdev(
							raid1BdevName, leg0, leg1,
							x.Raid1Conf.BitSizeKb)
					}
				}
			}
		default:
			logger.Warning("Unknow redundancy: %v", x)
			grpFeErr = fmt.Errorf("UnknowRedundancyError")
		}
	}

	raid0BdevName := sh.nf.Raid0BdevName(grpFeReq.GrpId)
	sh.raid0BdevMap[raid0BdevName] = true
	if grpFeErr == nil {
		if cntlrFeReq.CntlrFeConf.Raid0Conf.BdevCnt != uint32(
			len(raid0BdevList)) {
			grpFeErr = fmt.Errorf("Raid0LenMismatch: %v %v",
				cntlrFeReq.CntlrFeConf.Raid0Conf.BdevCnt,
				len(raid0BdevList))
		} else {
			grpFeErr = sh.oc.CreateRaid0Bdev(raid0BdevName,
				cntlrFeReq.CntlrFeConf.Raid0Conf.StripSizeKb,
				raid0BdevList)
		}
		
	}

	grpBdevName := sh.nf.GrpBdevName(grpFeReq.GrpId)
	sh.grpBdevMap[grpBdevName] = true
	if grpFeErr == nil {
		grpFeErr = sh.oc.CreateGrpBdev(grpBdevName, raid0BdevName)
	}

	grpRsp := &pbcn.GrpFeRsp{
		GrpId: grpFeReq.GrpId,
		GrpFeInfo: &pbcn.GrpFeInfo{
			ErrInfo: newErrInfo(grpFeErr),
		},
		VdFeRspList: vdFeRspList,
	}
	return grpRsp
}

func (sh *syncupHelper) syncupSnapFe(cntlrFeReq *pbcn.CntlrFeReq,
	snapFeReq *pbcn.SnapFeReq) *pbcn.SnapFeRsp {
	logger.Info("syncupSnapFe: %v", snapFeReq)
	var snapErr error
	daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
	snapshotName := sh.nf.SnapshotName(snapFeReq.SnapId)
	cloneName := sh.nf.CloneName(snapFeReq.SnapId)
	var oriName string
	if snapFeReq.SnapFeConf.OriId == "" {
		oriName = lib.MainLvName
	} else {
		oriName = sh.nf.CloneName(snapFeReq.SnapFeConf.OriId)
	}
	cloneFullName := sh.nf.CloneFullName(cntlrFeReq.CntlrFeConf.DaId,
		snapFeReq.SnapId)
	sh.snapMap[cloneFullName] = true
	if snapErr == nil {
		snapErr = sh.oc.CreateSnapshot(daLvsName, snapshotName, oriName)
	}
	if snapErr == nil {
		snapErr = sh.oc.CreateClone(daLvsName, cloneName, snapshotName)
	}
	if snapErr == nil {
		snapErr = sh.oc.ResizeLv(daLvsName, cloneName,
			snapFeReq.SnapFeConf.Size)
	}
	snapFeRsp := &pbcn.SnapFeRsp{
		SnapId: snapFeReq.SnapId,
		SnapFeInfo: &pbcn.SnapFeInfo{
			ErrInfo: newErrInfo(snapErr),
		},
	}
	return snapFeRsp
}

func (sh *syncupHelper) syncupExpFe(cntlrFeReq *pbcn.CntlrFeReq,
	expFeReq *pbcn.ExpFeReq, secNqnList []string) *pbcn.ExpFeRsp {
	logger.Info("syncupExpFe: %v", expFeReq)
	var expFeErr error
	expNqnName := sh.nf.ExpNqnName(expFeReq.ExpFeConf.DaName, expFeReq.ExpFeConf.ExpName)
	sh.expNqnMap[expNqnName] = true
	var lvFullName string
	if expFeReq.ExpFeConf.SnapId == "" {
		daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
		lvFullName = daLvsName + "/" + lib.MainLvName
	} else {
		lvFullName = sh.nf.CloneFullName(cntlrFeReq.CntlrFeConf.DaId,
			expFeReq.ExpFeConf.SnapId)
	}
	initiatorNqn := expFeReq.ExpFeConf.InitiatorNqn
	lisConf := sh.lisConf

	if expFeErr == nil {
		expFeErr = sh.oc.EnableHistogram(lvFullName)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateExpPrimaryNvmf(expNqnName,
			lvFullName, initiatorNqn, secNqnList, lisConf)
	}

	expFeRsp := &pbcn.ExpFeRsp{
		ExpId: expFeReq.ExpId,
		ExpFeInfo: &pbcn.ExpFeInfo{
			ErrInfo: newErrInfo(expFeErr),
		},
	}
	return expFeRsp
}

func (sh *syncupHelper) syncupPrimary(cntlrFeReq *pbcn.CntlrFeReq,
	secNqnList []string) *pbcn.CntlrFeRsp {
	logger.Info("syncupPrimary: %v", cntlrFeReq)
	var cntlrFeErr error
	grpFeRspList := make([]*pbcn.GrpFeRsp, 0)
	snapFeRspList := make([]*pbcn.SnapFeRsp, 0)
	expFeRspList := make([]*pbcn.ExpFeRsp, 0)

	sh.idxToMt = make(map[string]*pbcn.MtFeReq)
	for _, mtFeReq := range cntlrFeReq.MtFeReqList {
		key := mtKey(mtFeReq.MtFeConf.GrpIdx, mtFeReq.MtFeConf.VdIdx)
		sh.idxToMt[key] = mtFeReq
	}
	sh.mtFeRspList = make([]*pbcn.MtFeRsp, 0)

	grpFeReqList := cntlrFeReq.GrpFeReqList
	sort.Slice(grpFeReqList, func(i, j int) bool {
		return grpFeReqList[i].GrpFeConf.GrpIdx > grpFeReqList[j].GrpFeConf.GrpIdx
	})
	grpBdevList := make([]string, 0)
	for _, grpFeReq := range grpFeReqList {
		grpBdevName := sh.nf.GrpBdevName(grpFeReq.GrpId)
		grpBdevList = append(grpBdevList, grpBdevName)
		grpFeRsp := sh.syncupGrpFe(cntlrFeReq, grpFeReq)
		grpFeRspList = append(grpFeRspList, grpFeRsp)
		if grpFeRsp.GrpFeInfo.ErrInfo.IsErr {
			cntlrFeErr = fmt.Errorf("GrpFeError")
		}
	}

	aggBdevName := sh.nf.AggBdevName(cntlrFeReq.CntlrFeConf.DaId)
	sh.aggBdevMap[aggBdevName] = true
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.CreateAggBdev(aggBdevName, grpBdevList)
	}
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.ExamineBdev(aggBdevName)
	}
	daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
	sh.daLvsMap[daLvsName] = true
	if cntlrFeErr == nil {
		if cntlrFeReq.IsInited {
			cntlrFeErr = sh.oc.WaitForLvs(daLvsName)
		} else {
			cntlrFeErr = sh.oc.CreateDaLvs(daLvsName, aggBdevName,
				cntlrFeReq.CntlrFeConf.LvsConf.ClusterSize,
				cntlrFeReq.CntlrFeConf.LvsConf.ExtendRatio)
		}
		if cntlrFeErr == nil {
			sh.oc.CreateMainLv(daLvsName, cntlrFeReq.CntlrFeConf.Size)
		}
	}

	if cntlrFeErr == nil {
		snapFeReqList := cntlrFeReq.SnapFeReqList
		sort.Slice(snapFeReqList, func(i, j int) bool {
			return snapFeReqList[i].SnapFeConf.Idx > snapFeReqList[j].SnapFeConf.Idx
		})
		for _, snapFeReq := range snapFeReqList {
			snapFeRsp := sh.syncupSnapFe(cntlrFeReq, snapFeReq)
			snapFeRspList = append(snapFeRspList, snapFeRsp)
		}
	}

	if cntlrFeErr == nil {
		for _, expFeReq := range cntlrFeReq.ExpFeReqList {
			expFeRsp := sh.syncupExpFe(cntlrFeReq, expFeReq, secNqnList)
			expFeRspList = append(expFeRspList, expFeRsp)
		}
	}

	cntlrFeRsp := &pbcn.CntlrFeRsp{
		CntlrId: cntlrFeReq.CntlrId,
		CntlrFeInfo: &pbcn.CntlrFeInfo{
			ErrInfo: newErrInfo(cntlrFeErr),
		},
		GrpFeRspList:  grpFeRspList,
		SnapFeRspList: snapFeRspList,
		ExpFeRspList:  expFeRspList,
		MtFeRspList:   sh.mtFeRspList,
	}
	return cntlrFeRsp
}

func (sh *syncupHelper) syncupSecExpFe(cntlrFeReq *pbcn.CntlrFeReq,
	expFeReq *pbcn.ExpFeReq, primCntlr *pbcn.Controller) *pbcn.ExpFeRsp {
	logger.Info("syncupSecExpFe: %v", cntlrFeReq)
	var expFeErr error
	secNvmeName := sh.nf.SecNvmeName(expFeReq.ExpId)
	sh.secNvmeMap[secNvmeName] = true
	secBdevName := sh.nf.SecBdevName(expFeReq.ExpId)
	expNqnName := sh.nf.ExpNqnName(expFeReq.ExpFeConf.DaName, expFeReq.ExpFeConf.ExpName)
	sh.expNqnMap[expNqnName] = true
	secNqnName := sh.nf.SecNqnName(cntlrFeReq.CntlrId)
	primLisConf := &lib.LisConf{
		TrType:  primCntlr.CnNvmfListener.TrType,
		AdrFam:  primCntlr.CnNvmfListener.AdrFam,
		TrAddr:  primCntlr.CnNvmfListener.TrAddr,
		TrSvcId: primCntlr.CnNvmfListener.TrSvcId,
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateSecNvme(secNvmeName,
			expNqnName, secNqnName, primLisConf)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.EnableHistogram(secBdevName)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateExpSecNvmf(expNqnName,
			secBdevName, expFeReq.ExpFeConf.InitiatorNqn, sh.lisConf)
	}

	expFeRsp := &pbcn.ExpFeRsp{
		ExpId: expFeReq.ExpId,
		ExpFeInfo: &pbcn.ExpFeInfo{
			ErrInfo: newErrInfo(expFeErr),
		},
	}
	return expFeRsp

}

func (sh *syncupHelper) syncupSecondary(cntlrFeReq *pbcn.CntlrFeReq,
	primCntlr *pbcn.Controller) *pbcn.CntlrFeRsp {
	logger.Info("syncupSecondary: %v", cntlrFeReq)
	var cntlrFeErr error
	expFeRspList := make([]*pbcn.ExpFeRsp, 0)

	if cntlrFeErr == nil {
		for _, expFeReq := range cntlrFeReq.ExpFeReqList {
			expFeRsp := sh.syncupSecExpFe(cntlrFeReq, expFeReq, primCntlr)
			expFeRspList = append(expFeRspList, expFeRsp)
		}
	}

	cntlrFeRsp := &pbcn.CntlrFeRsp{
		CntlrId: cntlrFeReq.CntlrId,
		CntlrFeInfo: &pbcn.CntlrFeInfo{
			ErrInfo: newErrInfo(cntlrFeErr),
		},
		GrpFeRspList:  make([]*pbcn.GrpFeRsp, 0),
		SnapFeRspList: make([]*pbcn.SnapFeRsp, 0),
		ExpFeRspList:  expFeRspList,
		MtFeRspList:   make([]*pbcn.MtFeRsp, 0),
	}
	return cntlrFeRsp
}

func (sh *syncupHelper) syncupCntlrFe(cntlrFeReq *pbcn.CntlrFeReq) *pbcn.CntlrFeRsp {
	logger.Info("syncupCntlrFe: %v", cntlrFeReq)
	var cntlrFeErr error
	var thisCntlr *pbcn.Controller
	var primCntlr *pbcn.Controller
	secNqnList := make([]string, 0)
	for _, cntlr := range cntlrFeReq.CntlrFeConf.CntlrList {
		if cntlrFeReq.CntlrId == cntlr.CntlrId {
			thisCntlr = cntlr
		}
		if cntlr.IsPrimary {
			primCntlr = cntlr
		} else {
			secNqnName := sh.nf.SecNqnName(cntlr.CntlrId)
			secNqnList = append(secNqnList, secNqnName)
		}
	}

	if thisCntlr == nil {
		cntlrFeErr = fmt.Errorf("can not find thisCntlr")
	}
	if primCntlr == nil {
		cntlrFeErr = fmt.Errorf("can not find primCntlr")
	}

	if cntlrFeErr == nil {
		if thisCntlr.IsPrimary {
			return sh.syncupPrimary(cntlrFeReq, secNqnList)
		} else {
			return sh.syncupSecondary(cntlrFeReq, primCntlr)
		}
	} else {
		return &pbcn.CntlrFeRsp{
			CntlrId: cntlrFeReq.CntlrId,
			CntlrFeInfo: &pbcn.CntlrFeInfo{
				ErrInfo: newErrInfo(cntlrFeErr),
			},
			GrpFeRspList:  make([]*pbcn.GrpFeRsp, 0),
			SnapFeRspList: make([]*pbcn.SnapFeRsp, 0),
			ExpFeRspList:  make([]*pbcn.ExpFeRsp, 0),
			MtFeRspList:   make([]*pbcn.MtFeRsp, 0),
		}
	}
}

func (sh *syncupHelper) syncupCn(cnReq *pbcn.CnReq) *pbcn.CnRsp {
	var cnErr error

	logger.Info("syncupCn: %v", cnReq)
	cntlrFeRspList := make([]*pbcn.CntlrFeRsp, 0)
	cnErr = sh.oc.LoadNvmfs()

	if cnErr == nil {
		for _, cntlrFeReq := range cnReq.CntlrFeReqList {
			cntlrFeRsp := sh.syncupCntlrFe(cntlrFeReq)
			cntlrFeRspList = append(cntlrFeRspList, cntlrFeRsp)
		}
	}

	if cnErr == nil {
		cnErr = sh.oc.LoadBdevs()
	}

	if cnErr == nil {
		expNqnPrefix := sh.nf.ExpNqnPrefix()
		expNqnList, cnErr := sh.oc.GetExpNqnList(expNqnPrefix)
		if cnErr == nil {
			for _, expNqnName := range expNqnList {
				_, ok := sh.expNqnMap[expNqnName]
				if !ok {
					cnErr = sh.oc.DeleteExpNvmf(expNqnName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		secNvmfPrefix := sh.nf.SecNvmePrefix()
		secNvmeList, cnErr := sh.oc.GetSecNvmeList(secNvmfPrefix)
		if cnErr == nil {
			for _, secNvmeName := range secNvmeList {
				_, ok := sh.secNvmeMap[secNvmeName]
				if !ok {
					cnErr = sh.oc.DeleteSecNvme(secNvmeName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		toBeDeleted1 := make([]string, 0)
		toBeDeleted2 := make([]string, 0)
		snapFullNamePrefix := sh.nf.SnapFullNamePrefix()
		snapFullNameList, cnErr := sh.oc.GetSnapList(snapFullNamePrefix)
		for _, snapName := range snapFullNameList {
			if sh.nf.IsClone(snapName) {
				_, ok := sh.snapMap[snapName]
				if !ok {
					cnErr = sh.oc.DeleteSnap(snapName)
					if cnErr != nil {
						break
					}
					snapshotName := sh.nf.CloneToSnapshot(snapName)
					toBeDeleted1 = append(toBeDeleted1, snapshotName)
				}
			}
		}

		for {
			for _, snapshotName := range toBeDeleted1 {
				tmpErr := sh.oc.DeleteSnap(snapshotName)
				if tmpErr != nil {
					toBeDeleted2 = append(toBeDeleted2, snapshotName)
				}
			}
			if len(toBeDeleted1) == len(toBeDeleted2) {
				break
			}
			toBeDeleted1 = toBeDeleted2
			toBeDeleted2 = make([]string, 0)
		}
	}

	if cnErr == nil {
		daLvsPrefix := sh.nf.DaLvsPrefix()
		daLvsList, cnErr := sh.oc.GetDaLvsList(daLvsPrefix)
		if cnErr == nil {
			for _, daLvsName := range daLvsList {
				_, ok := sh.daLvsMap[daLvsName]
				if !ok {
					cnErr = sh.oc.DeleteDaLvs(daLvsName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		aggBdevPrefix := sh.nf.AggBdevPrefix()
		aggBdevList, cnErr := sh.oc.GetAggBdevList(aggBdevPrefix)
		if cnErr == nil {
			for _, aggBdevName := range aggBdevList {
				_, ok := sh.aggBdevMap[aggBdevName]
				if !ok {
					cnErr = sh.oc.DeleteAggBdev(aggBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		grpBdevPrefix := sh.nf.GrpBdevPrefix()
		grpBdevList, cnErr := sh.oc.GetGrpBdevList(grpBdevPrefix)
		if cnErr == nil {
			for _, grpBdevName := range grpBdevList {
				_, ok := sh.grpBdevMap[grpBdevName]
				if !ok {
					cnErr = sh.oc.DeleteGrpBdev(grpBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		raid0BdevPrefix := sh.nf.Raid0BdevPrefix()
		raid0BdevList, cnErr := sh.oc.GetRaid0BdevList(raid0BdevPrefix)
		if cnErr == nil {
			for _, raid0BdevName := range raid0BdevList {
				_, ok := sh.raid0BdevMap[raid0BdevName]
				if !ok {
					cnErr = sh.oc.DeleteRaid0Bdev(raid0BdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		raid1BdevPrefix := sh.nf.Raid1BdevPrefix()
		raid1BdevList, cnErr := sh.oc.GetRaid1BdevList(raid1BdevPrefix)
		if cnErr == nil {
			for _, raid1BdevName := range raid1BdevList {
				_, ok := sh.raid1BdevMap[raid1BdevName]
				if !ok {
					cnErr = sh.oc.DeleteRaid1Bdev(raid1BdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		vdSusresBdevPrefix := sh.nf.VdSusresPrefix()
		vdSusresBdevList, cnErr := sh.oc.GetVdSusresBdevList(
			vdSusresBdevPrefix)
		if cnErr == nil {
			for _, vdSusresBdevName := range vdSusresBdevList {
				_, ok := sh.vdSusresMap[vdSusresBdevName]
				if !ok {
					cnErr = sh.oc.DeleteVdSusresBdev(
						vdSusresBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		mtRaid1BdevPrevix := sh.nf.MtRaid1Prefix()
		mtRaid1BdevList, cnErr := sh.oc.GetMtRaid1BdevList(mtRaid1BdevPrevix)
		if cnErr == nil {
			for _, mtRaid1BdevName := range mtRaid1BdevList {
				_, ok := sh.mtRaid1Map[mtRaid1BdevName]
				if !ok {
					cnErr = sh.oc.DeleteMtRaid1Bdev(
						mtRaid1BdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		mtConcatBdevPrefix := sh.nf.MtConcatPrefix()
		mtConcatBdevList, cnErr := sh.oc.GetMtConcatBdevList(
			mtConcatBdevPrefix)
		if cnErr == nil {
			for _, mtConcatBdevName := range mtConcatBdevList {
				_, ok := sh.mtConcatMap[mtConcatBdevName]
				if !ok {
					cnErr = sh.oc.DeleteMtConcatBdev(
						mtConcatBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		mtNullBdevPrefix := sh.nf.MtNullPrefix()
		mtNullBdevList, cnErr := sh.oc.GetMtNullBdevList(mtNullBdevPrefix)
		if cnErr == nil {
			for _, mtNullBdevName := range mtNullBdevList {
				_, ok := sh.mtNullMap[mtNullBdevName]
				if !ok {
					cnErr = sh.oc.DeleteMtNullBdev(
						mtNullBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		feNvmePrefix := sh.nf.FeNvmePrefix()
		feNvmeList, cnErr := sh.oc.GetFeNvmeList(feNvmePrefix)
		if cnErr == nil {
			for _, feNvmeName := range feNvmeList {
				_, ok := sh.feNvmeMap[feNvmeName]
				if !ok {
					cnErr = sh.oc.DeleteFeNvme(feNvmeName)
					if cnErr != nil {
						break
					}
				}
			}
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
		vdSusresMap:  make(map[string]bool),
		mtNullMap:    make(map[string]bool),
		mtConcatMap:  make(map[string]bool),
		mtRaid1Map:   make(map[string]bool),
		aggBdevMap:   make(map[string]bool),
		daLvsMap:     make(map[string]bool),
		snapMap:      make(map[string]bool),
		expNqnMap:    make(map[string]bool),
		secNvmeMap:   make(map[string]bool),
		grpBdevMap:   make(map[string]bool),
		raid0BdevMap: make(map[string]bool),
		raid1BdevMap: make(map[string]bool),
	}
}

func (cnAgent *cnAgentServer) SyncupCn(ctx context.Context, req *pbcn.SyncupCnRequest) (
	*pbcn.SyncupCnReply, error) {
	if !cnMutex.TryLock() {
		return &pbcn.SyncupCnReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnLockErrCode,
				ReplyMsg: "Can not get lock",
			},
		}, nil
	}
	defer cnMutex.Unlock()
	logger.Debug("SyncupCn get lock")
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version < currVersion {
		return &pbcn.SyncupCnReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOldRevErrCode,
				ReplyMsg: fmt.Sprintf(
					"received rev: %d, current rev: %d",
					req.Version, currVersion),
			},
		}, nil
	}
	atomic.StoreUint64(&lastVersion, req.Version)
	sh := newSyncupHelper(cnAgent.lisConf, cnAgent.nf, cnAgent.sc)
	sh.reqId = req.ReqId

	cnRsp := sh.syncupCn(req.CnReq)
	return &pbcn.SyncupCnReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
		CnRsp: cnRsp,
	}, nil
}
