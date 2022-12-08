package cli

import (
	"github.com/spf13/cobra"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type daCreateArgsStruct struct {
	daName         string
	description    string
	cntlrCnt       uint32
	sizeMb         uint64
	stripCnt       uint32
	stripSizeKb    uint32
	rwIosPerSec    uint64
	rwMbytesPerSec uint64
	rMbytesPerSec  uint64
	wMbytesPerSec  uint64
	clusterSize    uint32
	extendRatio    uint32
	initGrpSizeMb  uint64
	maxGrpSizeMb   uint64
	lowWaterMarkMb uint64
	redundancy     string
	BitSizeKb      uint32
}

type daDeleteArgsStruct struct {
	daName string
}

type daModifyArgsStruct struct {
	daName string
	key    string
	value  string
}

type daListArgsStruct struct {
	limit int64
	token string
}

type daGetArgsStruct struct {
	daName string
}

var (
	daCmd = &cobra.Command{
		Use: "da",
	}

	daCreateCmd = &cobra.Command{
		Use:  "create",
		Args: cobra.MaximumNArgs(0),
		Run:  daCreateFunc,
	}
	daCreateArgs = &daCreateArgsStruct{}

	daDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  daDeleteFunc,
	}
	daDeleteArgs = &daDeleteArgsStruct{}

	daModifyCmd = &cobra.Command{
		Use:  "modify",
		Args: cobra.MaximumNArgs(0),
		Run:  daModifyFunc,
	}
	daModifyArgs = &daModifyArgsStruct{}

	daListCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  daListFunc,
	}
	daListArgs = &daListArgsStruct{}

	daGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  daGetFunc,
	}
	daGetArgs = &daGetArgsStruct{}
)

func init() {
	daCreateCmd.Flags().StringVarP(&daCreateArgs.daName, "da-name", "", "",
		"da name")
	daCreateCmd.MarkFlagRequired("da-name")
	daCreateCmd.Flags().StringVarP(&daCreateArgs.description, "description", "", "",
		"da description")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.cntlrCnt, "cntlr-cnt", "", 0,
		"da controller count")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.sizeMb, "size-mb", "", 0,
		"da size in MB")
	daCreateCmd.MarkFlagRequired("size")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.stripCnt, "strip-cnt", "", 0,
		"da strip count")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.stripSizeKb, "strip-size-kb", "", 0,
		"da strip size in KB")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.rwIosPerSec, "rw-ios-per-sec", "", 0,
		"da read/write ios per second")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.rwMbytesPerSec, "rw-mbytes-per-sec", "", 0,
		"da read/write mbytes per second")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.rMbytesPerSec, "r-mbytes-per-sec", "", 0,
		"da read mbytes per second")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.wMbytesPerSec, "w-mbytes-per-sec", "", 0,
		"da write mbytes per second")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.clusterSize, "cluster-size", "", 0,
		"cluster size of the logical volume store in bytes")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.extendRatio, "extend-ratio", "", 0,
		"reserved metadata pages per cluster")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.initGrpSizeMb, "init-grp-size-mb", "", 0,
		"the init group size")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.maxGrpSizeMb, "max-grp-size-mb", "", 0,
		"the max group size")
	daCreateCmd.Flags().Uint64VarP(&daCreateArgs.lowWaterMarkMb, "low-water-mark-mb", "", 0,
		"the low water mark in MB")
	daCreateCmd.Flags().StringVarP(&daCreateArgs.redundancy, "redundancy", "", "",
		"redundancy type, current the only valid value is raid1")
	daCreateCmd.Flags().Uint32VarP(&daCreateArgs.BitSizeKb, "bit-size-kb", "", 0,
		"the bit size in kb of raid1")
	daCmd.AddCommand(daCreateCmd)

	daDeleteCmd.Flags().StringVarP(&daDeleteArgs.daName, "da-name", "", "",
		"da name")
	daDeleteCmd.MarkFlagRequired("da-name")
	daCmd.AddCommand(daDeleteCmd)

	daModifyCmd.Flags().StringVarP(&daModifyArgs.daName, "da-name", "", "",
		"da name")
	daModifyCmd.MarkFlagRequired("da-name")
	daModifyCmd.Flags().StringVarP(&daModifyArgs.key, "key", "", "",
		"key to modify, current can only be description")
	daModifyCmd.MarkFlagRequired("key")
	daModifyCmd.Flags().StringVarP(&daModifyArgs.value, "value", "", "",
		"value of of the key")
	daModifyCmd.MarkFlagRequired("value")
	daCmd.AddCommand(daModifyCmd)

	daListCmd.Flags().Int64VarP(&daListArgs.limit, "limit", "", 0,
		"max return items")
	daListCmd.Flags().StringVarP(&daListArgs.token, "token", "", "",
		"the token returned by previous list cmd")
	daCmd.AddCommand(daListCmd)

	daGetCmd.Flags().StringVarP(&daGetArgs.daName, "da-name", "", "",
		"da name")
	daGetCmd.MarkFlagRequired("da-name")
	daCmd.AddCommand(daGetCmd)
}

func (cli *client) createDa(args *daCreateArgsStruct) string {
	req := &pbpo.CreateDaRequest{
		DaName:       args.daName,
		Description:  args.description,
		CntlrCnt:     args.cntlrCnt,
		DaConf: &pbpo.DaConf{
			Size: args.sizeMb * 1024 * 1024,
			Qos: &pbpo.BdevQos{
				RwIosPerSec:    args.rwIosPerSec,
				RwMbytesPerSec: args.rwMbytesPerSec,
				RMbytesPerSec:  args.rMbytesPerSec,
				WMbytesPerSec:  args.wMbytesPerSec,
			},
			ExtendPolicy: &pbpo.ExtendPolicy{
				InitGrpSize: args.initGrpSizeMb * 1024 * 1024,
				MaxGrpSize: args.maxGrpSizeMb * 1024 * 1024,
				LowWaterMark: args.lowWaterMarkMb * 1024 * 1024,
			},
			LvsConf: &pbpo.LvsConf{
				ClusterSize: args.clusterSize,
				ExtendRatio: args.extendRatio,
			},
			Raid0Conf: &pbpo.Raid0Conf{
				StripSizeKb: args.stripSizeKb,
				BdevCnt: args.stripCnt,
			},
		},
	}
	reply, err := cli.c.CreateDa(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) deleteDa(args *daDeleteArgsStruct) string {
	req := &pbpo.DeleteDaRequest{
		DaName: args.daName,
	}
	reply, err := cli.c.DeleteDa(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) modifyDa(args *daModifyArgsStruct) string {
	req := &pbpo.ModifyDaRequest{}
	req.DaName = args.daName
	if args.key == "description" {
		req.Attr = &pbpo.ModifyDaRequest_Description{
			Description: args.value,
		}
	} else {
		return "Unknown key"
	}
	reply, err := cli.c.ModifyDa(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) listDa(args *daListArgsStruct) string {
	req := &pbpo.ListDaRequest{
		Limit: args.limit,
		Token: args.token,
	}
	reply, err := cli.c.ListDa(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) getDa(args *daGetArgsStruct) string {
	req := &pbpo.GetDaRequest{
		DaName: daGetArgs.daName,
	}
	reply, err := cli.c.GetDa(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func daCreateFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.createDa(daCreateArgs)
	cli.show(output)
}

func daDeleteFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.deleteDa(daDeleteArgs)
	cli.show(output)
}

func daModifyFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.modifyDa(daModifyArgs)
	cli.show(output)
}

func daListFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.listDa(daListArgs)
	cli.show(output)
}

func daGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getDa(daGetArgs)
	cli.show(output)
}
