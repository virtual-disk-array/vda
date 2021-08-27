package cnagent

import (
	"context"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

type metricsHelper struct {
	nf *lib.NameFmt
	oc *lib.OperationClient
}

func (mh *metricsHelper) BdevGetMetrics() []*pbcn.BdevMetrics {
	var res []*pbcn.BdevMetrics
	snapFullNamePrefix := mh.nf.SnapFullNamePrefix()
	snapFullNameList, cnErr := mh.oc.GetSnapList(snapFullNamePrefix)
	if cnErr == nil {
		for _, snapFullName := range snapFullNameList {
			metrics := &pbcn.BdevMetrics{
				SnapFullName: snapFullName,
			}
			mh.getIostatAndHistogram(snapFullName, metrics)
			res = append(res, metrics)
		}
	}

	secNvmfPrefix := mh.nf.SecNvmePrefix()
	secNvmeList, cnErr := mh.oc.GetSecNvmeList(secNvmfPrefix)
	if cnErr == nil {
		for _, secNvmeName := range secNvmeList {
			metrics := &pbcn.BdevMetrics{
				ExpId: mh.nf.ExpId(secNvmeName),
			}
			secBdevName := mh.nf.SecNvmeNameToBdevName(secNvmeName)
			mh.getIostatAndHistogram(secBdevName, metrics)
			res = append(res, metrics)
		}
	}
	return res
}

func (mh *metricsHelper) getIostatAndHistogram(bdevName string, metrics *pbcn.BdevMetrics) error {
	iostat, err := mh.oc.BdevGetIostat(bdevName)
	if err != nil {
		logger.Error("operationclient BdevGetIostat failed: %v", err)
		return err
	}
	ios := &pbcn.BdevIostat{
		TickRate:                iostat.TickRate,
		BytesRead:               iostat.BytesRead,
		NumReadOps:              iostat.NumReadOps,
		BytesWritten:            iostat.BytesWritten,
		NumWriteOps:             iostat.NumWriteOps,
		BytesUnmapped:           iostat.BytesUnmapped,
		NumUnmapOps:             iostat.NumUnmapOps,
		ReadLatencyTicks:        iostat.ReadLatencyTicks,
		WriteLatencyTicks:       iostat.WriteLatencyTicks,
		UnmapLatencyTicks:       iostat.UnmapLatencyTicks,
		QueueDepthPollingPeriod: iostat.QueueDepthPollingPeriod,
		QueueDepth:              iostat.QueueDepth,
		IoTime:                  iostat.IoTime,
		WeightedIoTime:          iostat.WeightedIoTime,
	}
	metrics.BdevIostat = ios

	histogram, err := mh.oc.BdevGetHistogram(bdevName)
	if err != nil {
		logger.Error("operationclient BdevGetHistogram failed: %v", err)
		return err
	}
	histo := &pbcn.BdevHistogram{
		Histogram:   histogram.Histogram,
		TscRate:     histogram.TscRate,
		BucketShift: histogram.BucketShift,
	}
	metrics.BdevHistogram = histo
	return nil
}

func newMetricsHelper(nf *lib.NameFmt, sc *lib.SpdkClient) *metricsHelper {
	mh := &metricsHelper{
		nf: nf,
		oc: lib.NewOperationClient(sc),
	}
	mh.oc.LoadBdevs()
	return mh
}

func (cnAgent *cnAgentServer) BdevGetMetrics(ctx context.Context, req *pbcn.BdevMetricsRequest) (*pbcn.BdevMetricsReply, error) {
	mh := newMetricsHelper(cnAgent.nf, cnAgent.sc)
	bdevMetrics := mh.BdevGetMetrics()
	return &pbcn.BdevMetricsReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
		BdevMetrics: bdevMetrics,
	}, nil
}
