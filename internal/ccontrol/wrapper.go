package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"strconv"
)

func ParseWrapped() error {
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
				println("!!!")
				println(args) //TODO:DELETE ME
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
			Use:   "job",
			Short: "display the state of a specified job or all jobs",
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

	rootCmd.AddCommand(showCmd)
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")
	isSlurm := false
	rootCmd.PersistentFlags().BoolVar(&isSlurm, "slurm", false, "use it as a slurm cmd")
	showCmd.AddCommand(showNodeCmd)
	showCmd.AddCommand(showPartitionCmd)
	showCmd.AddCommand(showTaskCmd)
	rootCmd.AddCommand(updateCmd)
	updateCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Job id")
	updateCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "time limit")
	err := updateCmd.MarkFlagRequired("job")
	return err
}
