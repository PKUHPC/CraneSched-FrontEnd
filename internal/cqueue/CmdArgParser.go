package cqueue

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	partition string

	rootCmd = &cobra.Command{
		Use:   "cqueue [partition]",
		Short: "display the job information for all queues in the cluster",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			// args was checked by cobra.ExactArgs(1)
			// len(args)=1 here.
			partition = args[0]
			Query(partition)
		},
	}
)

func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
