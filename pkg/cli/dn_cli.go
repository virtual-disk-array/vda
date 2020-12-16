package cli

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

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
	dnCreateArgs = dnCreateArgsStruct{}

	dnDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  dnDeleteFunc,
	}
	dnDeleteArgs = dnDeleteArgsStruct{}

	dnGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  dnGetFunc,
	}
	dnGetArgs = dnGetArgsStruct{}
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

func dnCreateFunc(cmd *cobra.Command, args []string) {
	log.Println(rootArgs)
	log.Println(dnCreateArgs)

	conn, err := grpc.Dial(rootArgs.portalAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pbpo.NewPortalClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(), time.Duration(rootArgs.portalTimeout)*time.Second)
	defer cancel()

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
	}
	reply, err := c.CreateDn(ctx, req)
	log.Println(reply)
}

func dnDeleteFunc(cmd *cobra.Command, args []string) {
	log.Println(rootArgs)
	log.Println(dnDeleteArgs)
}

func dnGetFunc(cmd *cobra.Command, args []string) {
	log.Println(rootArgs)
	log.Println(dnGetArgs)

	conn, err := grpc.Dial(rootArgs.portalAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pbpo.NewPortalClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(), time.Duration(rootArgs.portalTimeout)*time.Second)
	defer cancel()
	req := &pbpo.GetDnRequest{
		SockAddr: dnGetArgs.sockAddr,
	}
	reply, err := c.GetDn(ctx, req)
	log.Println(reply)
}
