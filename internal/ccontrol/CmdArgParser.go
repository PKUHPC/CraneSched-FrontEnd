package ccontrol

import (
	"CraneFrontEnd/internal/util"
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
		Short: "display the state of partitions and nodes",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Preparation()
		},
	}
	showCmd = &cobra.Command{
		Use:   "show",
		Short: "display state of identified entity, default is all records",
		Long:  "",
	}
	showNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "display state of the specified node, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
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
		Short: "display state of the specified partition, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
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
		Short: "display state of the specified job, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				queryAll = true
			} else {
				id, _ := strconv.Atoi(args[0])
				jobId = uint32(id)
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
	rootCmd.PersistentFlags().StringVarP(&util.ConfigFilePath, "config", "C", "/etc/crane/config.yaml", "Path to configuration file")
	showCmd.AddCommand(showNodeCmd)
	showCmd.AddCommand(showPartitionCmd)
	showCmd.AddCommand(showJobCmd)
}
