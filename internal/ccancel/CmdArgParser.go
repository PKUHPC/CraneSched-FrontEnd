package ccancel

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	taskId uint32

	rootCmd = &cobra.Command{
		Use:   "ccancel",
		Short: "A command to cancel the specified task",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			CancelTask(taskId)
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
	rootCmd.Flags().Uint32VarP(&taskId, "taskId", "T", 0, "the id of the task")
}
