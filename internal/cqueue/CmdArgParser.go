package cqueue

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	iterate   uint64
	noheader  bool
	partition string
	format    string
	taskId    string
	states    string
	taskName  string
	rootCmd   = &cobra.Command{
		Use:   "cqueue",
		Short: "display the job information for all queues in the cluster",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			if iterate != 0 {
				IterateQuery(iterate)
			} else if format != "" {
				FormatQuery(format)
			} else if taskId != "" || states != "" || partition != "" || taskName != "" {
				FilterQuery()
			} else {
				DefaultQuery()
			}
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
	rootCmd.Flags().BoolVarP(&noheader, "noheader", "N", false,
		"no headers on output")
	rootCmd.Flags().Uint64VarP(&iterate, "iterate", "i", 0,
		"specify an interation period in seconds")
	rootCmd.Flags().StringVarP(&format, "format", "o", "",
		"format specification")
	rootCmd.Flags().StringVarP(&taskId, "job", "j", "",
		"comma separated list of tasks IDs to view,\ndefault is all")
	rootCmd.Flags().StringVarP(&partition, "partition", "p", "",
		"comma separated list of partitions to view,\ndefault is all partitions")
	rootCmd.Flags().StringVarP(&states, "states", "t", "",
		"Specify a states to view, default is  all states")
	rootCmd.Flags().StringVarP(&taskName, "name", "n", "",
		"comma separated list of task names to view,\ndefault is all")
}
