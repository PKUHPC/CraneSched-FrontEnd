package ccancel

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	name      string
	partition string
	state     string
	rootCmd   = &cobra.Command{
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
	rootCmd.Flags().StringVarP(&name, "name", "n", "", "act only on jobs with this name")
	rootCmd.Flags().StringVarP(&partition, "partition", "p", "", "act only on jobs in this partition")
	rootCmd.Flags().StringVarP(&state, "state", "t", "", "act only on jobs in this state.  Valid job\nstates are PENDING, RUNNING")
}
