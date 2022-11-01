package ccancel

import (
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	rootCmd = &cobra.Command{
		Use:   "ccancel",
		Short: "cancel the specified task",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Init()
		},
		Run: func(cmd *cobra.Command, args []string) {
			taskId64, _ := strconv.ParseUint(args[0], 10, 32)
			CancelTask(uint32(taskId64))
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
