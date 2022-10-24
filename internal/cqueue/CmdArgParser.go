package cqueue

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	partition string

	rootCmd = &cobra.Command{
		Use:   "cqueue",
		Short: "A command to show the job information for all queues in the cluster.",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			findAll := true
			if partition != "" {
				findAll = false
			}
			Query(partition, findAll)
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
	rootCmd.Flags().StringVarP(&partition, "partition", "P", "", "specify a partition name, default is all partitions")
}
