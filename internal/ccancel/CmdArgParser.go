package ccancel

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	taskName  string //单个.
	partition string //单个.
	state     string //单个. 默认值
	account   string //单个.
	userName  string //单个.
	nodes     string //多个

	rootCmd = &cobra.Command{
		Use:   "ccancel",
		Short: "cancel the specified task",
		Long:  "",
		//Args:  cobra.MinimumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			//taskId64, _ := strconv.ParseUint(args[0], 10, 32)
			CancelTask()
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
	rootCmd.Flags().StringVarP(&taskName, "taskName", "n", "",
		"act only on jobs with this taskName")
	rootCmd.Flags().StringVarP(&partition, "partition", "p", "",
		"act only on jobs in this partition")
	rootCmd.Flags().StringVarP(&state, "state", "t", "",
		"act only on jobs in this state.  Valid job\nstates are PENDING, RUNNING")
	rootCmd.Flags().StringVarP(&account, "account", "A", "",
		"act only on jobs charging this account")
	rootCmd.Flags().StringVarP(&userName, "user", "u", "",
		"act only on jobs of this user")
	rootCmd.Flags().StringVarP(&nodes, "nodeList", "w", "",
		" act only on jobs on these nodes")
}
