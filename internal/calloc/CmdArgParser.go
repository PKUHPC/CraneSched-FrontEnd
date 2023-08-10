package calloc

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
)

var (
	FlagNodes         uint32
	FlagCpuPerTask    float64
	FlagGpus		  uint64
	FlagNtasksPerNode uint32
	FlagTime          string
	FlagMem           string
	FlagPartition     string
	FlagJob           string
	FlagAccount       string
	FlagQos           string
	FlagCwd           string

	FlagConfigFilePath string
)

func CmdArgParser() *cobra.Command {
	parser := &cobra.Command{
		Use:   "calloc",
		Short: "allocate resource and create terminal",
		Run: func(cmd *cobra.Command, args []string) {
			main(cmd, args)
		},
	}

	parser.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	parser.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, " number of nodes on which to run (N = min[-max])")
	parser.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "number of cpus required per task")
	parser.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "number of tasks to invoke on each node")
	parser.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
	parser.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory")
	parser.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	parser.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	parser.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	parser.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	parser.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
	parser.Flags().Uint64Var(&FlagGpus, "gpus", 0, "num of gpus required per task")
	return parser
}
