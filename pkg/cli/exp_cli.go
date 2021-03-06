package cli

import (
	"github.com/spf13/cobra"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type expCreateArgsStruct struct {
	daName       string
	expName      string
	description  string
	initiatorNqn string
	snapName     string
}

type expDeleteArgsStruct struct {
	daName  string
	expName string
}

type expModifyArgsStruct struct {
	daName  string
	expName string
	key     string
	value   string
}

type expListArgsStruct struct {
	daName string
}

type expGetArgsStruct struct {
	daName  string
	expName string
}

var (
	expCmd = &cobra.Command{
		Use: "exp",
	}

	expCreateCmd = &cobra.Command{
		Use:  "create",
		Args: cobra.MaximumNArgs(0),
		Run:  expCreateFunc,
	}
	expCreateArgs = &expCreateArgsStruct{}

	expDeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.MaximumNArgs(0),
		Run:  expDeleteFunc,
	}
	expDeleteArgs = &expDeleteArgsStruct{}

	expModifyCmd = &cobra.Command{
		Use:  "modify",
		Args: cobra.MaximumNArgs(0),
		Run:  expModifyFunc,
	}
	expModifyArgs = &expModifyArgsStruct{}

	expListCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  expListFunc,
	}
	expListArgs = &expListArgsStruct{}

	expGetCmd = &cobra.Command{
		Use:  "get",
		Args: cobra.MaximumNArgs(0),
		Run:  expGetFunc,
	}
	expGetArgs = &expGetArgsStruct{}
)

func init() {
	expCreateCmd.Flags().StringVarP(&expCreateArgs.daName, "da-name", "", "",
		"da name")
	expCreateCmd.MarkFlagRequired("da-name")
	expCreateCmd.Flags().StringVarP(&expCreateArgs.expName, "exp-name", "", "",
		"exp name")
	expCreateCmd.MarkFlagRequired("exp-name")
	expCreateCmd.Flags().StringVarP(&expCreateArgs.description, "description", "", "",
		"exp description")
	expCreateCmd.Flags().StringVarP(&expCreateArgs.initiatorNqn, "initiator-nqn", "", "",
		"initiator nqn")
	expCreateCmd.MarkFlagRequired("initiator-nqn")
	expCreateCmd.Flags().StringVarP(&expCreateArgs.snapName, "snap-name", "", "",
		"snap name")
	expCmd.AddCommand(expCreateCmd)

	expDeleteCmd.Flags().StringVarP(&expDeleteArgs.daName, "da-name", "", "",
		"da name")
	expDeleteCmd.MarkFlagRequired("da-name")
	expDeleteCmd.Flags().StringVarP(&expDeleteArgs.expName, "exp-name", "", "",
		"exp name")
	expDeleteCmd.MarkFlagRequired("exp-name")
	expCmd.AddCommand(expDeleteCmd)

	expModifyCmd.Flags().StringVarP(&expModifyArgs.daName, "da-name", "", "",
		"da name")
	expModifyCmd.MarkFlagRequired("da-name")
	expModifyCmd.Flags().StringVarP(&expModifyArgs.expName, "exp-name", "", "",
		"exp name")
	expModifyCmd.MarkFlagRequired("exp-name")
	expModifyCmd.Flags().StringVarP(&expModifyArgs.key, "key", "", "",
		"key to modify, only support one key: description")
	expModifyCmd.MarkFlagRequired("key")
	expModifyCmd.Flags().StringVarP(&expModifyArgs.value, "value", "", "",
		"value of the key")
	expModifyCmd.MarkFlagRequired("value")
	expCmd.AddCommand(expModifyCmd)

	expListCmd.Flags().StringVarP(&expListArgs.daName, "da-name", "", "",
		"da name")
	expListCmd.MarkFlagRequired("da-name")
	expCmd.AddCommand(expListCmd)

	expGetCmd.Flags().StringVarP(&expGetArgs.daName, "da-name", "", "",
		"da name")
	expGetCmd.MarkFlagRequired("da-name")
	expGetCmd.Flags().StringVarP(&expGetArgs.expName, "exp-name", "", "",
		"exp name")
	expGetCmd.MarkFlagRequired("exp-name")
	expCmd.AddCommand(expGetCmd)
}

func (cli *client) createExp(args *expCreateArgsStruct) string {
	req := &pbpo.CreateExpRequest{
		DaName:       args.daName,
		ExpName:      args.expName,
		Description:  args.description,
		InitiatorNqn: args.initiatorNqn,
		SnapName:     args.snapName,
	}
	reply, err := cli.c.CreateExp(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) deleteExp(args *expDeleteArgsStruct) string {
	req := &pbpo.DeleteExpRequest{
		DaName:  args.daName,
		ExpName: args.expName,
	}
	reply, err := cli.c.DeleteExp(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) modifyExp(args *expModifyArgsStruct) string {
	req := &pbpo.ModifyExpRequest{
		DaName:  args.daName,
		ExpName: args.expName,
	}
	if args.key == "description" {
		req.Attr = &pbpo.ModifyExpRequest_Description{
			Description: args.value,
		}
	} else {
		return "Unknown key"
	}
	reply, err := cli.c.ModifyExp(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) listExp(args *expListArgsStruct) string {
	req := &pbpo.ListExpRequest{
		DaName: args.daName,
	}
	reply, err := cli.c.ListExp(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func (cli *client) getExp(args *expGetArgsStruct) string {
	req := &pbpo.GetExpRequest{
		DaName:  args.daName,
		ExpName: args.expName,
	}
	reply, err := cli.c.GetExp(cli.ctx, req)
	if err != nil {
		return err.Error()
	} else {
		return cli.serialize(reply)
	}
}

func expCreateFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.createExp(expCreateArgs)
	cli.show(output)
}

func expDeleteFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.deleteExp(expDeleteArgs)
	cli.show(output)
}

func expModifyFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.modifyExp(expModifyArgs)
	cli.show(output)
}

func expListFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.listExp(expListArgs)
	cli.show(output)
}

func expGetFunc(cmd *cobra.Command, args []string) {
	cli := newClient(rootArgs)
	defer cli.close()
	output := cli.getExp(expGetArgs)
	cli.show(output)
}
