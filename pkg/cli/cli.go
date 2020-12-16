package cli

import (
	"log"

	"github.com/spf13/cobra"
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
	rootArgs = rootArgsStruct{}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&rootArgs.portalAddr, "portal-addr", "", "localhost:9520", "portal socket address")
	rootCmd.PersistentFlags().IntVarP(
		&rootArgs.portalTimeout, "portal-timeout", "", 30, "portal timeout")
	// rootCmd.AddCommand(dnCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
