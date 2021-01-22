package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type rootArgsStruct struct {
	portalAddr    string
	portalTimeout int
}

var (
	rootCmd = &cobra.Command{
		Use:   "vdacli",
		Short: "vda cli",
		Long:  `vda cli`,
	}
	rootArgs = &rootArgsStruct{}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&rootArgs.portalAddr, "portal-addr", "", "localhost:9520", "portal socket address")
	rootCmd.PersistentFlags().IntVarP(
		&rootArgs.portalTimeout, "portal-timeout", "", 30, "portal timeout")
	rootCmd.AddCommand(dnCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Fatal("Execute err: %v", err)
	}
}

type client struct {
	conn   *grpc.ClientConn
	c      pbpo.PortalClient
	ctx    context.Context
	cancel context.CancelFunc
}

func (cli *client) close() {
	cli.cancel()
	cli.conn.Close()
}

func (cli *client) serialize(reply interface{}) string {
	output, err := json.MarshalIndent(reply, "", "  ")
	if err != nil {
		return err.Error()
	} else {
		return string(output)
	}
}

func (cli *client) show(output string) {
	fmt.Println(output)
}

func newClient(args *rootArgsStruct) *client {
	conn, err := grpc.Dial(args.portalAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logger.Fatal("Connection err: %v %v", args, err)
	}
	c := pbpo.NewPortalClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(args.portalTimeout)*time.Second)
	return &client{
		conn:   conn,
		c:      c,
		ctx:    ctx,
		cancel: cancel,
	}
}
