package cqueue

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	partition string

	rootCmd = &cobra.Command{
		Use:   "cqueue [partition]",
		Short: "display the job information for all queues in the cluster",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Preparation()
		},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				partition = args[0]
			}
			Query(partition)
		},
	}
)

func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&util.ConfigFilePath, "config", "C", "/etc/crane/config.yaml", "Path to configuration file")
}
