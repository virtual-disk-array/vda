package cli

import (
	"fmt"

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

type dnGetArgsStruct struct {
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

	dnGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  dnGetFunc,
	}
	dnGetArgs = &dnGetArgsStruct{}
)

func init() {
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnCreateCmd.MarkFlagRequired("sock-addr")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.description, "description", "", "",
		"dn description")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trType, "tr-type", "", "",
		"nvmf listener tr type")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.adrFam, "adr-fam", "", "",
		"nvmf listener adr fam")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trAddr, "tr-addr", "", "",
		"nvmf listener tr addr")
	dnCreateCmd.Flags().StringVarP(&dnCreateArgs.trSvcId, "tr-svc-id", "", "",
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

	dnGetCmd.Flags().StringVarP(&dnGetArgs.sockAddr, "sock-addr", "", "",
		"dn socket address")
	dnGetCmd.MarkFlagRequired("sock-addr")
	dnCmd.AddCommand(dnGetCmd)

}

func (cli *client) createDn(args *dnCreateArgsStruct) string {
	req := &pbpo.CreateDnRequest{
		SockAddr:    dnCreateArgs.sockAddr,
		Description: dnCreateArgs.description,
		NvmfListener: &pbpo.NvmfListener{
			TrType:  dnCreateArgs.trType,
			AdrFam:  dnCreateArgs.adrFam,
			TrAddr:  dnCreateArgs.trAddr,
			TrSvcId: dnCreateArgs.trSvcId,
		},
		Location:  dnCreateArgs.location,
		IsOffline: dnCreateArgs.isOffline,
		HashCode:  dnCreateArgs.hashCode,
	}
	reply, err := cli.c.CreateDn(cli.ctx, req)
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
	fmt.Println(output)
}

func dnDeleteFunc(cmd *cobra.Command, args []string) {
}

func (cli *client) getDn(args *dnGetArgsStruct) string {
	req := &pbpo.GetDnRequest{
		SockAddr: dnGetArgs.sockAddr,
	}
	reply, err := cli.c.GetDn(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func dnGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getDn(dnGetArgs)
	fmt.Println(output)
}
