package dnagent

import (
	"context"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type metricsHelper struct {
	nf *lib.NameFmt
	oc *lib.OperationClient
}

func (mh *metricsHelper) BdevGetMetrics() []*pbdn.BdevMetrics {
	pdBdevPrefix := mh.nf.PdBdevPrefix()
	pdBdevList, dnErr := mh.oc.GetPdBdevList(pdBdevPrefix)
	var res []*pbdn.BdevMetrics
	if dnErr == nil {
		for _, bdevName := range pdBdevList {
			pdId := mh.nf.PdId(bdevName)
			metrics := &pbdn.BdevMetrics{
				PdId: pdId,
			}
			iostat, err := mh.oc.BdevGetIostat(bdevName)
			if err != nil {
				logger.Error("operationclient BdevGetIostat failed: %v", err)
			} else {
				ios := &pbdn.BdevIostat{
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
			}
			histogram, err := mh.oc.BdevGetHistogram(bdevName)
			if err != nil {
				logger.Error("operationclient BdevGetHistogram failed: %v", err)
			} else {
				histo := &pbdn.BdevHistogram{
					Histogram:   histogram.Histogram,
					TscRate:     histogram.TscRate,
					BucketShift: histogram.BucketShift,
				}
				metrics.BdevHistogram = histo
			}
			res = append(res, metrics)
		}
	}
	return res
}

func newMetricsHelper(nf *lib.NameFmt, sc *lib.SpdkClient) *metricsHelper {
	mh := &metricsHelper{
		nf: nf,
		oc: lib.NewOperationClient(sc),
	}
	mh.oc.LoadBdevs()
	return mh
}

func (dnAgent *dnAgentServer) BdevGetMetrics(ctx context.Context, req *pbdn.BdevMetricsRequest) (*pbdn.BdevMetricsReply, error) {
	mh := newMetricsHelper(dnAgent.nf, dnAgent.sc)
	bdevMetrics := mh.BdevGetMetrics()
	return &pbdn.BdevMetricsReply{
		ReplyInfo: &pbdn.ReplyInfo{
			ReplyCode: lib.DnSucceedCode,
			ReplyMsg:  lib.DnSucceedMsg,
		},
		BdevMetrics: bdevMetrics,
	}, nil
}
