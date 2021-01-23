package cli

import (
	"strconv"

	"github.com/spf13/cobra"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type pdCreateArgsStruct struct {
	sockAddr       string
	pdName         string
	description    string
	isOffline      bool
	rwIosPerSec    uint64
	rwMbytesPerSec uint64
	rMbytesPerSec  uint64
	wMbytesPerSec  uint64
	bdevTypeKey    string
	bdevTypeValue  string
}

type pdDeleteArgsStruct struct {
	sockAddr string
	pdName   string
}

type pdModifyArgsStruct struct {
	sockAddr string
	pdName   string
	key      string
	value    string
}

type pdListArgsStruct struct {
	sockAddr string
}

type pdGetArgsStruct struct {
	sockAddr string
	pdName   string
}

var (
	pdCmd = &cobra.Command{
		Use: "pd",
	}

	pdCreateCmd = &cobra.Command{
		Use:  "create",
		Args: cobra.MaximumNArgs(0),
		Run:  pdCreateFunc,
	}
	pdCreateArgs = &pdCreateArgsStruct{}

	pdDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  pdDeleteFunc,
	}
	pdDeleteArgs = &pdDeleteArgsStruct{}

	pdModifyCmd = &cobra.Command{
		Use:  "modify",
		Args: cobra.MaximumNArgs(0),
		Run:  pdModifyFunc,
	}
	pdModifyArgs = &pdModifyArgsStruct{}

	pdListCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  pdListFunc,
	}
	pdListArgs = &pdListArgsStruct{}

	pdGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  pdGetFunc,
	}
	pdGetArgs = &pdGetArgsStruct{}
)

func init() {
	pdCreateCmd.Flags().StringVarP(&pdCreateArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	pdCreateCmd.MarkFlagRequired("sock-addr")
	pdCreateCmd.Flags().StringVarP(&pdCreateArgs.pdName, "pd-name", "", "",
		"pd name")
	pdCreateCmd.MarkFlagRequired("pd-name")
	pdCreateCmd.Flags().StringVarP(&pdCreateArgs.description, "description", "", "",
		"pd description")
	pdCreateCmd.Flags().BoolVarP(&pdCreateArgs.isOffline, "is-offline", "", false,
		"whether pd is offline")
	pdCreateCmd.Flags().Uint64VarP(&pdCreateArgs.rwIosPerSec, "rw-ios-per-sec", "", 0,
		"rw ios per sec")
	pdCreateCmd.Flags().Uint64VarP(&pdCreateArgs.rwMbytesPerSec, "rw-mbytes-per-sec", "", 0,
		"rw mbytes per sec")
	pdCreateCmd.Flags().Uint64VarP(&pdCreateArgs.rMbytesPerSec, "r-mbytes-per-sec", "", 0,
		"r mbytes per sec")
	pdCreateCmd.Flags().Uint64VarP(&pdCreateArgs.wMbytesPerSec, "w-mbytes-per-sec", "", 0,
		"w mbytes per sec")
	pdCreateCmd.Flags().StringVarP(&pdCreateArgs.bdevTypeKey, "bdev-type-key", "", "",
		"bdev type key, one of these three values: malloc, aio, nvme")
	pdCreateCmd.MarkFlagRequired("bdev-type-key")
	pdCreateCmd.Flags().StringVarP(&pdCreateArgs.bdevTypeValue, "bdev-type-value", "", "",
		"bdev type values, size in MB for malloc, file/dev path for aio, pcie addr for nvme")
	pdCreateCmd.MarkFlagRequired("bdev-type-value")
	pdCmd.AddCommand(pdCreateCmd)

	pdDeleteCmd.Flags().StringVarP(&pdDeleteArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	pdDeleteCmd.MarkFlagRequired("sock-addr")
	pdDeleteCmd.Flags().StringVarP(&pdDeleteArgs.pdName, "pd-name", "", "",
		"pd name")
	pdDeleteCmd.MarkFlagRequired("pd-name")
	pdCmd.AddCommand(pdDeleteCmd)

	pdModifyCmd.Flags().StringVarP(&pdModifyArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	pdModifyCmd.MarkFlagRequired("sock-addr")
	pdModifyCmd.Flags().StringVarP(&pdModifyArgs.pdName, "pd-name", "", "",
		"pd name")
	pdModifyCmd.MarkFlagRequired("pd-name")
	pdModifyCmd.Flags().StringVarP(&pdModifyArgs.key, "key", "", "",
		"key to modify, one of: description, isOffline")
	pdModifyCmd.MarkFlagRequired("key")
	pdModifyCmd.Flags().StringVarP(&pdModifyArgs.value, "value", "", "",
		"value of of the key")
	pdModifyCmd.MarkFlagRequired("value")
	pdCmd.AddCommand(pdModifyCmd)

	pdListCmd.Flags().StringVarP(&pdListArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	pdListCmd.MarkFlagRequired("sock-addr")
	pdCmd.AddCommand(pdListCmd)

	pdGetCmd.Flags().StringVarP(&pdGetArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	pdGetCmd.MarkFlagRequired("sock-addr")
	pdGetCmd.Flags().StringVarP(&pdGetArgs.pdName, "pd-name", "", "",
		"pd name")
	pdGetCmd.MarkFlagRequired("pd-name")
	pdCmd.AddCommand(pdGetCmd)
}

func (cli *client) createPd(args *pdCreateArgsStruct) string {
	req := &pbpo.CreatePdRequest{
		SockAddr:    args.sockAddr,
		PdName:      args.pdName,
		Description: args.description,
		IsOffline:   args.isOffline,
		BdevQos: &pbpo.BdevQos{
			RwIosPerSec:    args.rwIosPerSec,
			RwMbytesPerSec: args.rwMbytesPerSec,
			RMbytesPerSec:  args.rMbytesPerSec,
			WMbytesPerSec:  args.wMbytesPerSec,
		},
	}
	if args.bdevTypeKey == "malloc" {
		sizeMb, err := strconv.ParseUint(args.bdevTypeValue, 10, 64)
		if err != nil {
			return "Can not convert bdevTypeValue to uint64: " + err.Error()
		}
		size := sizeMb * uint64(1024*1024)
		req.BdevType = &pbpo.CreatePdRequest_BdevMalloc{
			BdevMalloc: &pbpo.BdevMalloc{
				Size: size,
			},
		}
	} else if args.bdevTypeKey == "aio" {
		req.BdevType = &pbpo.CreatePdRequest_BdevAio{
			BdevAio: &pbpo.BdevAio{
				FileName: args.bdevTypeValue,
			},
		}
	} else if args.bdevTypeKey == "nvme" {
		req.BdevType = &pbpo.CreatePdRequest_BdevNvme{
			BdevNvme: &pbpo.BdevNvme{
				TrAddr: args.bdevTypeValue,
			},
		}
	}
	reply, err := cli.c.CreatePd(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) deletePd(args *pdDeleteArgsStruct) string {
	req := &pbpo.DeletePdRequest{
		SockAddr: args.sockAddr,
		PdName:   args.pdName,
	}
	reply, err := cli.c.DeletePd(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) modifyPd(args *pdModifyArgsStruct) string {
	req := &pbpo.ModifyPdRequest{
		SockAddr: args.sockAddr,
		PdName:   args.pdName,
	}
	if args.key == "description" {
		req.Attr = &pbpo.ModifyPdRequest_Description{
			Description: args.value,
		}
	} else if args.key == "isOffline" {
		isOffline, err := strconv.ParseBool(args.value)
		if err != nil {
			return "Can not convert isOffline to bool: " + err.Error()
		}
		req.Attr = &pbpo.ModifyPdRequest_IsOffline{
			IsOffline: isOffline,
		}
	} else {
		return "Unknonw key"
	}
	reply, err := cli.c.ModifyPd(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) listPd(args *pdListArgsStruct) string {
	req := &pbpo.ListPdRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.ListPd(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) getPd(args *pdGetArgsStruct) string {
	req := &pbpo.GetPdRequest{
		SockAddr: args.sockAddr,
		PdName:   args.pdName,
	}
	reply, err := cli.c.GetPd(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func pdCreateFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.createPd(pdCreateArgs)
	cli.show(output)
}

func pdDeleteFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.deletePd(pdDeleteArgs)
	cli.show(output)
}

func pdModifyFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.modifyPd(pdModifyArgs)
	cli.show(output)
}

func pdListFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.listPd(pdListArgs)
	cli.show(output)
}

func pdGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getPd(pdGetArgs)
	cli.show(output)
}
