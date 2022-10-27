package ccontrol

import (
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	nodeName      string
	partitionName string
	jobId         uint32
	queryAll      bool

	rootCmd = &cobra.Command{
		Use:   "ccontrol",
		Short: "A command to show the status of partitions and nodes.",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
	}
	showCmd = &cobra.Command{
		Use:   "show",
		Short: "A command to perform the show operation.",
		Long:  "",
	}
	showNodeCmd = &cobra.Command{ //可以指定nodename(arg[0])也可以没有，显示所有
		Use:   "node",
		Short: "A command to show the status of nodes.",
		Long: "Specify a node name: ccontrol show node [name], " +
			"otherwise all node status will be displayed.",
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				nodeName = ""
				queryAll = true
			} else {
				nodeName = args[0]
				queryAll = false
			}
			ShowNodes(nodeName, queryAll)
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: " A command to show the status of partitions.",
		Long: " Specify a partition:  ccontrol show partition [name], " +
			" otherwise all partition status will be displayed.",
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				partitionName = ""
				queryAll = true
			} else {
				partitionName = args[0]
				queryAll = false
			}
			ShowPartitions(partitionName, queryAll)
		},
	}
	showJobCmd = &cobra.Command{
		Use:   "job",
		Short: "A command to show the status of jobs.",
		Long: "Specify a job name:  ccontrol show node [job], " +
			"   otherwise all job status will be displayed.",
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				queryAll = true
			} else {
				int, _ := strconv.Atoi(args[0])
				jobId = uint32(int)
				queryAll = false
			}
			ShowJobs(jobId, queryAll)
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
	rootCmd.AddCommand(showCmd)
	showCmd.AddCommand(showNodeCmd)
	showCmd.AddCommand(showPartitionCmd)
	showCmd.AddCommand(showJobCmd)
}
