package cbatch

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	nodes         uint32
	cpuPerTask    float64
	ntasksPerNode uint32
	time          string
	mem           string
	partition     string
	job           string
	output        string
	rootCmd       = &cobra.Command{
		Use:   "cbatch",
		Short: "submit batch jobs",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			Cbatch(args[0])
		},
	}
)

func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().Uint32VarP(&nodes, "nodes", "N", 1, " number of nodes on which to run (N = min[-max])")
	rootCmd.Flags().Float64VarP(&cpuPerTask, "cpus-per-task", "c", 1, "number of cpus required per task")
	rootCmd.Flags().Uint32Var(&ntasksPerNode, "ntasks-per-node", 1, "number of tasks to invoke on each node")
	rootCmd.Flags().StringVarP(&time, "time", "t", "", "time limit")
	rootCmd.Flags().StringVar(&mem, "mem", "", "minimum amount of real memory")
	rootCmd.Flags().StringVarP(&partition, "partition", "p", "", "partition requested")
	rootCmd.Flags().StringVarP(&output, "output", "o", "", "file for batch script's standard output")
	rootCmd.Flags().StringVarP(&job, "job-name", "J", "", "name of job")
}
