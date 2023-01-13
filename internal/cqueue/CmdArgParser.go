package cqueue

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	partition string
	rootCmd   = &cobra.Command{
		Use:   "cqueue",
		Short: "display the job information for all queues in the cluster",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				partition = ""
			} else {
				partition = args[0]
			}
			Query(partition)
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
