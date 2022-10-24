package cinfo

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cinfo",
		Short: "A command to show the status of all partitions and nodes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			cinfoFun()
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
func init() {
}
