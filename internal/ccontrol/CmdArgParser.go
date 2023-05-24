package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	FlagNodeName      string
	FlagPartitionName string
	FlagTaskId        uint32
	FlagQueryAll      bool
	FlagTimeLimit     string

	FlagConfigFilePath string

	rootCmd = &cobra.Command{
		Use:   "ccontrol",
		Short: "display the state of partitions and nodes",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
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
				FlagNodeName = ""
				FlagQueryAll = true
			} else {
				FlagNodeName = args[0]
				FlagQueryAll = false
			}
			ShowNodes(FlagNodeName, FlagQueryAll)
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "display state of the specified partition, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagPartitionName = ""
				FlagQueryAll = true
			} else {
				FlagPartitionName = args[0]
				FlagQueryAll = false
			}
			ShowPartitions(FlagPartitionName, FlagQueryAll)
		},
	}
	showTaskCmd = &cobra.Command{
		Use:   "task",
		Short: "display the state of a specified task or all tasks",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagQueryAll = true
			} else {
				id, _ := strconv.Atoi(args[0])
				FlagTaskId = uint32(id)
				FlagQueryAll = false
			}
			ShowTasks(FlagTaskId, FlagQueryAll)
		},
	}
	updateCmd = &cobra.Command{
		Use:   "update",
		Short: "Modify job information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ChangeTaskTimeLimit(FlagTaskId, FlagTimeLimit)
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
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")
	showCmd.AddCommand(showNodeCmd)
	showCmd.AddCommand(showPartitionCmd)
	showCmd.AddCommand(showTaskCmd)
	rootCmd.AddCommand(updateCmd)
	updateCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Job id")
	updateCmd.Flags().StringVarP(&FlagTimeLimit, "time_limit", "T", "", "time limit")
	err := updateCmd.MarkFlagRequired("job")
	if err != nil {
		return
	}
}
