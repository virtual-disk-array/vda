package cli

import (
	"strconv"

	"github.com/spf13/cobra"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type cnCreateArgsStruct struct {
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

type cnDeleteArgsStruct struct {
	sockAddr string
}

type cnModifyArgsStruct struct {
	sockAddr string
	key      string
	value    string
}

type cnListArgsStruct struct {
	limit int64
	token string
}

type cnGetArgsStruct struct {
	sockAddr string
}

type cnSyncupArgsStruct struct {
	sockAddr string
}

var (
	cnCmd = &cobra.Command{
		Use: "cn",
	}

	cnCreateCmd = &cobra.Command{
		Use:  "create",
		Args: cobra.MaximumNArgs(0),
		Run:  cnCreateFunc,
	}
	cnCreateArgs = &cnCreateArgsStruct{}

	cnDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  cnDeleteFunc,
	}
	cnDeleteArgs = &cnDeleteArgsStruct{}

	cnModifyCmd = &cobra.Command{
		Use:  "modify",
		Args: cobra.MaximumNArgs(0),
		Run:  cnModifyFunc,
	}
	cnModifyArgs = &cnModifyArgsStruct{}

	cnListCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  cnListFunc,
	}
	cnListArgs = &cnListArgsStruct{}

	cnGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  cnGetFunc,
	}
	cnGetArgs = &cnGetArgsStruct{}

	cnSyncupCmd = &cobra.Command{
		Use:  "syncup",
		Args: cobra.MaximumNArgs(0),
		Run:  cnSyncupFunc,
	}
	cnSyncupArgs = &cnSyncupArgsStruct{}
)

func init() {
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.sockAddr, "sock-addr", "", "",
		"cn socket address")
	cnCreateCmd.MarkFlagRequired("sock-addr")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.description, "description", "", "",
		"cn description")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.trType, "tr-type", "", "tcp",
		"nvmf listener tr type")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.adrFam, "adr-fam", "", "ipv4",
		"nvmf listener adr fam")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.trAddr, "tr-addr", "", "127.0.0.1",
		"nvmf listener tr addr")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.trSvcId, "tr-svc-id", "", "4430",
		"nvmf listener tr svc id")
	cnCreateCmd.Flags().StringVarP(&cnCreateArgs.location, "location", "", "",
		"cn location")
	cnCreateCmd.Flags().BoolVarP(&cnCreateArgs.isOffline, "is-offline", "", false,
		"whether cn is offline")
	cnCreateCmd.Flags().Uint32VarP(&cnCreateArgs.hashCode, "hash-code", "", 0,
		"hash code of the cn")
	cnCmd.AddCommand(cnCreateCmd)

	cnDeleteCmd.Flags().StringVarP(&cnDeleteArgs.sockAddr, "sock-addr", "", "",
		"cn socket address")
	cnDeleteCmd.MarkFlagRequired("sock-addr")
	cnCmd.AddCommand(cnDeleteCmd)

	cnModifyCmd.Flags().StringVarP(&cnModifyArgs.sockAddr, "sock-addr", "", "",
		"cn socket address")
	cnModifyCmd.MarkFlagRequired("sock-addr")
	cnModifyCmd.Flags().StringVarP(&cnModifyArgs.key, "key", "", "",
		"key to modify, one of: description, isOffline, hashCode")
	cnModifyCmd.MarkFlagRequired("key")
	cnModifyCmd.Flags().StringVarP(&cnModifyArgs.value, "value", "", "",
		"value of of the key")
	cnModifyCmd.MarkFlagRequired("value")
	cnCmd.AddCommand(cnModifyCmd)

	cnListCmd.Flags().Int64VarP(&cnListArgs.limit, "limit", "", 0,
		"max return items")
	cnListCmd.Flags().StringVarP(&cnListArgs.token, "token", "", "",
		"the token returned by previous list cmd")
	cnCmd.AddCommand(cnListCmd)

	cnGetCmd.Flags().StringVarP(&cnGetArgs.sockAddr, "sock-addr", "", "",
		"cn socket address")
	cnGetCmd.MarkFlagRequired("sock-addr")
	cnCmd.AddCommand(cnGetCmd)

	cnSyncupCmd.Flags().StringVarP(&cnSyncupArgs.sockAddr, "sock-addr", "", "",
		"cn socket address")
	cnSyncupCmd.MarkFlagRequired("sock-addr")
	cnCmd.AddCommand(cnSyncupCmd)
}

func (cli *client) createCn(args *cnCreateArgsStruct) string {
	req := &pbpo.CreateCnRequest{
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
	reply, err := cli.c.CreateCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) deleteCn(args *cnDeleteArgsStruct) string {
	req := &pbpo.DeleteCnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.DeleteCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) modifyCn(args *cnModifyArgsStruct) string {
	req := &pbpo.ModifyCnRequest{}
	req.SockAddr = args.sockAddr
	if args.key == "description" {
		req.Attr = &pbpo.ModifyCnRequest_Description{
			Description: args.value,
		}
	} else if args.key == "isOffline" {
		isOffline, err := strconv.ParseBool(args.value)
		if err != nil {
			return "Can not convert isOffline to bool: " + err.Error()
		}
		req.Attr = &pbpo.ModifyCnRequest_IsOffline{
			IsOffline: isOffline,
		}
	} else if args.key == "hashCode" {
		hashCode, err := strconv.ParseUint(args.value, 10, 32)
		if err != nil {
			return "Can not convert hashCode to uint32: " + err.Error()
		}
		req.Attr = &pbpo.ModifyCnRequest_HashCode{
			HashCode: uint32(hashCode),
		}
	} else {
		return "Unknown key"
	}
	reply, err := cli.c.ModifyCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) listCn(args *cnListArgsStruct) string {
	req := &pbpo.ListCnRequest{
		Limit: args.limit,
		Token: args.token,
	}
	reply, err := cli.c.ListCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) getCn(args *cnGetArgsStruct) string {
	req := &pbpo.GetCnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.GetCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) syncupCn(args *cnSyncupArgsStruct) string {
	req := &pbpo.SyncupCnRequest{
		SockAddr: args.sockAddr,
	}
	reply, err := cli.c.SyncupCn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func cnCreateFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.createCn(cnCreateArgs)
	cli.show(output)
}

func cnDeleteFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.deleteCn(cnDeleteArgs)
	cli.show(output)
}

func cnModifyFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.modifyCn(cnModifyArgs)
	cli.show(output)
}

func cnListFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.listCn(cnListArgs)
	cli.show(output)
}

func cnGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getCn(cnGetArgs)
	cli.show(output)
}

func cnSyncupFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.syncupCn(cnSyncupArgs)
	cli.show(output)
}
