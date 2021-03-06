package cli

import (
	"strconv"

	"github.com/spf13/cobra"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type dnCreateArgsStruct struct {
	sockAddr    string
	description string
	trType      string
	adrFam      string
	trAddr      string
	trSvcId     string
	location    string
	isOffline   bool
	hashCode    uint32
}

type dnDeleteArgsStruct struct {
	sockAddr string
}

type dnModifyArgsStruct struct {
	sockAddr string
	key      string
	value    string
}

type dnListArgsStruct struct {
	limit int64
	token string
}

type dnGetArgsStruct struct {
	sockAddr string
}

type dnSyncupArgsStruct struct {
	sockAddr string
}

var (
	dnCmd = &cobra.Command{
		Use: "dn",
	}

	dnCreateCmd = &cobra.Command{
		Use:  "create",
		Args: cobra.MaximumNArgs(0),
		Run:  dnCreateFunc,
	}
	dnCreateArgs = &dnCreateArgsStruct{}

	dnDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  dnDeleteFunc,
	}
	dnDeleteArgs = &dnDeleteArgsStruct{}

	dnModifyCmd = &cobra.Command{
		Use:  "modify",
		Args: cobra.MaximumNArgs(0),
		Run:  dnModifyFunc,
	}
	dnModifyArgs = &dnModifyArgsStruct{}

	dnListCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  dnListFunc,
	}
	dnListArgs = &dnListArgsStruct{}

	dnGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  dnGetFunc,
	}
	dnGetArgs = &dnGetArgsStruct{}

	dnSyncupCmd = &cobra.Command{
		Use:  "syncup",
		Args: cobra.MaximumNArgs(0),
		Run:  dnSyncupFunc,
	}
	dnSyncupArgs = &dnSyncupArgsStruct{}
)

func init() {
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnCreateCmd.MarkFlagRequired("sock-addr")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.description, "description", "", "",
		"dn description")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trType, "tr-type", "", "tcp",
		"nvmf listener tr type")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.adrFam, "adr-fam", "", "ipv4",
		"nvmf listener adr fam")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trAddr, "tr-addr", "", "127.0.0.1",
		"nvmf listener tr addr")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trSvcId, "tr-svc-id", "", "4420",
		"nvmf listener tr svc id")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.location, "location", "", "",
		"dn location")
	dnCreateCmd.Flags().BoolVarP(&dnCreateArgs.isOffline, "is-offline", "", false,
		"whether dn is offline")
	dnCreateCmd.Flags().Uint32VarP(&dnCreateArgs.hashCode, "hash-code", "", 0,
		"hash code of the dn")
	dnCmd.AddCommand(dnCreateCmd)

	dnDeleteCmd.Flags().StringVarP(&dnDeleteArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnDeleteCmd.MarkFlagRequired("sock-addr")
	dnCmd.AddCommand(dnDeleteCmd)

	dnModifyCmd.Flags().StringVarP(&dnModifyArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnModifyCmd.MarkFlagRequired("sock-addr")
	dnModifyCmd.Flags().StringVarP(&dnModifyArgs.key, "key", "", "",
		"key to modify, one of: description, isOffline, hashCode")
	dnModifyCmd.MarkFlagRequired("key")
	dnModifyCmd.Flags().StringVarP(&dnModifyArgs.value, "value", "", "",
		"value of of the key")
	dnModifyCmd.MarkFlagRequired("value")
	dnCmd.AddCommand(dnModifyCmd)

	dnListCmd.Flags().Int64VarP(&dnListArgs.limit, "limit", "", 0,
		"max return items")
	dnListCmd.Flags().StringVarP(&dnListArgs.token, "token", "", "",
		"the token returned by previous list cmd")
	dnCmd.AddCommand(dnListCmd)

	dnGetCmd.Flags().StringVarP(&dnGetArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnGetCmd.MarkFlagRequired("sock-addr")
	dnCmd.AddCommand(dnGetCmd)

	dnSyncupCmd.Flags().StringVarP(&dnSyncupArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnSyncupCmd.MarkFlagRequired("sock-addr")
	dnCmd.AddCommand(dnSyncupCmd)
}

func (cli *client) createDn(args *dnCreateArgsStruct) string {
	req := &pbpo.CreateDnRequest{
		SockAddr:    args.sockAddr,
		Description: args.description,
		NvmfListener: &pbpo.NvmfListener{
			TrType:  args.trType,
			AdrFam:  args.adrFam,
			TrAddr:  args.trAddr,
			TrSvcId: args.trSvcId,
		},
		Location:  args.location,
		IsOffline: args.isOffline,
		HashCode:  args.hashCode,
	}
	reply, err := cli.c.CreateDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) deleteDn(args *dnDeleteArgsStruct) string {
	req := &pbpo.DeleteDnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.DeleteDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) modifyDn(args *dnModifyArgsStruct) string {
	req := &pbpo.ModifyDnRequest{}
	req.SockAddr = args.sockAddr
	if args.key == "description" {
		req.Attr = &pbpo.ModifyDnRequest_Description{
			Description: args.value,
		}
	} else if args.key == "isOffline" {
		isOffline, err := strconv.ParseBool(args.value)
		if err != nil {
			return "Can not convert isOffline to bool: " + err.Error()
		}
		req.Attr = &pbpo.ModifyDnRequest_IsOffline{
			IsOffline: isOffline,
		}
	} else if args.key == "hashCode" {
		hashCode, err := strconv.ParseUint(args.value, 10, 32)
		if err != nil {
			return "Can not convert hashCode to uint32: " + err.Error()
		}
		req.Attr = &pbpo.ModifyDnRequest_HashCode{
			HashCode: uint32(hashCode),
		}
	} else {
		return "Unknown key"
	}
	reply, err := cli.c.ModifyDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) listDn(args *dnListArgsStruct) string {
	req := &pbpo.ListDnRequest{
		Limit: args.limit,
		Token: args.token,
	}
	reply, err := cli.c.ListDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) getDn(args *dnGetArgsStruct) string {
	req := &pbpo.GetDnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.GetDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) syncupDn(args *dnSyncupArgsStruct) string {
	req := &pbpo.SyncupDnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.SyncupDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func dnCreateFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.createDn(dnCreateArgs)
	cli.show(output)
}

func dnDeleteFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.deleteDn(dnDeleteArgs)
	cli.show(output)
}

func dnModifyFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.modifyDn(dnModifyArgs)
	cli.show(output)
}

func dnListFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.listDn(dnListArgs)
	cli.show(output)
}

func dnGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getDn(dnGetArgs)
	cli.show(output)
}

func dnSyncupFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.syncupDn(dnSyncupArgs)
	cli.show(output)
}
