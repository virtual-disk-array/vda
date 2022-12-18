package lib

import (
	"fmt"
	"strings"
)

const (
	pdBdevType    = "000"
	pdLvsType     = "001"
	vdBeBdevType  = "002"
	vdFeNvmeType  = "003"
	aggBdevType   = "004"
	daLvsType     = "005"
	secNvmeType   = "006"
	grpBdevType   = "007"
	vdSusresType  = "008"
	mtNullType    = "009"
	mtConcatType  = "010"
	mtRaid1Type   = "011"
	raid0BdevType = "100"
	raid1BdevType = "101"

	beNqnPrefix  = "be"
	feNqnPrefix  = "fe"
	expNqnPrefix = "exp"
	secNqnPrefix = "sec"
)

type NameFmt struct {
	vdaPrefix string
	nqnPrefix string
}

func (nf *NameFmt) PdBdevPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, pdBdevType)
}

func (nf *NameFmt) PdLvsPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, pdLvsType)
}

func (nf *NameFmt) BeLvolPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, pdLvsType)
}

func (nf *NameFmt) BeNqnPrefix() string {
	return fmt.Sprintf("%s:%s", nf.nqnPrefix, beNqnPrefix)
}

func (nf *NameFmt) FeNqnPrefix() string {
	return fmt.Sprintf("%s:%s", nf.nqnPrefix, feNqnPrefix)
}

func (nf *NameFmt) FeNvmePrefix() string {
	return fmt.Sprintf("%s:%s", nf.vdaPrefix, vdFeNvmeType)
}

func (nf *NameFmt) AggBdevPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, aggBdevType)
}

func (nf *NameFmt) DaLvsPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, daLvsType)
}

func (nf *NameFmt) SnapFullNamePrefix() string {
	return nf.DaLvsPrefix()
}

func (nf *NameFmt) SecNvmePrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, secNvmeType)
}

func (nf *NameFmt) GrpBdevPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, grpBdevType)
}

func (nf *NameFmt) Raid0BdevPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, raid0BdevType)
}

func (nf *NameFmt) Raid1BdevPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, raid1BdevType)
}

func (nf *NameFmt) ExpNqnPrefix() string {
	return fmt.Sprintf("%s:%s", nf.nqnPrefix, expNqnPrefix)
}

func (nf *NameFmt) SecNqnPrefix() string {
	return fmt.Sprintf("%s:%s", nf.nqnPrefix, secNqnPrefix)
}

func (nf *NameFmt) PdBdevName(pdId string) string {
	return fmt.Sprintf("%s-%s", nf.PdBdevPrefix(), pdId)
}

func (nf *NameFmt) PdId(pdBdevName string) string {
	pdId := strings.Replace(pdBdevName, nf.PdBdevPrefix()+"-", "", 1)
	pdId = strings.Replace(pdId, "n1", "", 1) // in the case of Nvme type bdevName
	return pdId
}

func (nf *NameFmt) PdLvsName(pdId string) string {
	return fmt.Sprintf("%s-%s", nf.PdLvsPrefix(), pdId)
}

func (nf *NameFmt) BeLvolName(vdId string) string {
	return fmt.Sprintf("%s", vdId)
}

func (nf *NameFmt) BeLvolFullName(pdId, vdId string) string {
	lvsName := nf.PdLvsName(pdId)
	lvolName := nf.BeLvolName(vdId)
	return fmt.Sprintf("%s/%s", lvsName, lvolName)
}

func (nf *NameFmt) BeNqnName(vdId string) string {
	return fmt.Sprintf("%s-%s", nf.BeNqnPrefix(), vdId)
}

func (nf *NameFmt) FeNqnName(cntlrId string) string {
	return fmt.Sprintf("%s-%s", nf.FeNqnPrefix(), cntlrId)
}

func (nf *NameFmt) FeNvmeName(vdId string) string {
	return fmt.Sprintf("%s-%s", nf.FeNvmePrefix(), vdId)
}

func (nf *NameFmt) FeBdevName(vdId string) string {
	return fmt.Sprintf("%sn1", nf.FeNvmeName(vdId))
}

func (nf *NameFmt) AggBdevName(daId string) string {
	return fmt.Sprintf("%s-%s", nf.AggBdevPrefix(), daId)
}

func (nf *NameFmt) DaLvsName(daId string) string {
	return fmt.Sprintf("%s-%s", nf.DaLvsPrefix(), daId)
}

func (nf *NameFmt) Raid0BdevName(grpId string) string {
	return fmt.Sprintf("%s-%s", nf.Raid0BdevPrefix(), grpId)
}

func (nf *NameFmt) Raid1BdevName(leg0, leg1 string) string {
	return fmt.Sprintf("%s-%s%s", nf.Raid1BdevPrefix(),
		leg0[0:len(leg0)/2], leg1[len(leg1)/2:len(leg1)])
}

func (nf *NameFmt) GrpBdevName(grpId string) string {
	return fmt.Sprintf("%s-%s", nf.GrpBdevPrefix(), grpId)
}

func (nf *NameFmt) SnapshotPrefix() string {
	return "snapshot"
}

func (nf *NameFmt) SnapshotName(snapId string) string {
	return fmt.Sprintf("%s-%s", nf.SnapshotPrefix(), snapId)
}

func (nf *NameFmt) SnapshotFullName(daId, snapId string) string {
	return fmt.Sprintf("%s/%s", nf.DaLvsName(daId), nf.SnapshotName(snapId))
}

func (nf *NameFmt) ClonePrefix() string {
	return "clone"
}

func (nf *NameFmt) CloneName(snapId string) string {
	return fmt.Sprintf("%s-%s", nf.ClonePrefix(), snapId)
}

func (nf *NameFmt) CloneFullName(daId, snapId string) string {
	return fmt.Sprintf("%s/%s", nf.DaLvsName(daId), nf.CloneName(snapId))
}

func (nf *NameFmt) IsClone(bdevName string) bool {
	return strings.Contains(bdevName, nf.ClonePrefix())
}

func (nf *NameFmt) CloneToSnapshot(bdevName string) string {
	return strings.Replace(bdevName, nf.ClonePrefix(),  nf.SnapshotPrefix(), 1)
}

func (nf *NameFmt) ExpNqnName(daName, expName string) string {
	return fmt.Sprintf("%s-%s-%s", nf.ExpNqnPrefix(), daName, expName)
}

func (nf *NameFmt) SecNqnName(cntlrId string) string {
	return fmt.Sprintf("%s-%s", nf.SecNqnPrefix(), cntlrId)
}

func (nf *NameFmt) SecNvmeName(expId string) string {
	return fmt.Sprintf("%s-%s", nf.SecNvmePrefix(), expId)
}

func (nf *NameFmt) ExpId(secNvmeName string) string {
	return strings.Replace(secNvmeName, nf.SecNvmePrefix()+"-", "", 1)
}

func (nf *NameFmt) SecBdevName(expId string) string {
	return fmt.Sprintf("%sn1", nf.SecNvmeName(expId))
}

func (nf *NameFmt) SecNvmeNameToBdevName(secNvmeName string) string {
	return fmt.Sprintf("%sn1", secNvmeName)
}

func (nf *NameFmt) VdSusresPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, vdSusresType)
}

func (nf *NameFmt) VdSusresName(daId string, grpIdx uint32, vdIdx uint32) string {
	return fmt.Sprintf("%s-%s-%d-%d", nf.VdSusresPrefix(), daId, grpIdx, vdIdx)
}

func (nf *NameFmt) MtNullPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, mtNullType)
}

func (nf *NameFmt) MtNullName(vdId string) string {
	return fmt.Sprintf("%s-%s", nf.MtNullPrefix(), vdId)
}

func (nf *NameFmt) MtConcatPrefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, mtConcatType)
}

func (nf *NameFmt) MtConcatName(vdId string) string {
	return fmt.Sprintf("%s-%s", nf.MtConcatPrefix(), vdId)
}

func (nf *NameFmt) MtRaid1Prefix() string {
	return fmt.Sprintf("%s-%s", nf.vdaPrefix, mtRaid1Type)
}

func (nf *NameFmt) MtRaid1Name(mtId string) string {
	return fmt.Sprintf("%s-%s", nf.MtRaid1Prefix(), mtId)
}

func NewNameFmt(vdaPrefix, nqnPrefix string) *NameFmt {
	return &NameFmt{
		vdaPrefix: vdaPrefix,
		nqnPrefix: nqnPrefix,
	}
}
