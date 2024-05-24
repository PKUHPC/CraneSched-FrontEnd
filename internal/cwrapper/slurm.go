package cwrapper

import (
	"CraneFrontEnd/internal/ccontrol"
	"CraneFrontEnd/internal/cqueue"
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var slurmGroup = &cobra.Group{
	ID:    "slurm",
	Title: "Slurm Commands:",
}

func squeue() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "squeue",
		Short:   "Wrapper of cqueue command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			cqueue.RootCmd.Run(cmd, args)
		},
	}

	// As --noheader will use -h, we need to add the help flag manually
	cmd.Flags().BoolP("help", "", false, "Help for this command.")

	cmd.Flags().BoolVarP(&cqueue.FlagNoHeader, "noheader", "h", false, "Do not print a header on the output.")
	cmd.Flags().BoolVarP(&cqueue.FlagStartTime, "start", "S", false, "Report the expected start time and resources to be allocated for pending jobs in order of increasing start time.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterPartitions, "partition", "p", "", "Specify the partitions of the jobs or steps to view. Accepts a comma separated list of partition names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobIDs, "jobs", "j", "", "Specify a comma separated list of job IDs to display. Defaults to all jobs. ")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobNames, "name", "n", "", "Request jobs or job steps having one of the specified names. The list consists of a comma separated list of job names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterQos, "qos", "q", "", "Specify the qos(s) of the jobs or steps to view. Accepts a comma separated list of qos's.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterStates, "states", "t", "", "Specify the states of jobs to view. Accepts a comma separated list of state names or \"all\".")
	cmd.Flags().StringVarP(&cqueue.FlagFilterUsers, "user", "u", "", "Request jobs or job steps from a comma separated list of users.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterAccounts, "account", "A", "", "Specify the accounts of the jobs to view. Accepts a comma separated list of account names.")
	cmd.Flags().Uint64VarP(&cqueue.FlagIterate, "iterate", "i", 0, "Repeatedly gather and report the requested information at the interval specified (in seconds). By default, prints a time stamp with the header.")

	// The following flags are not supported by the wrapper
	// --format, -o: As the cqueue's output format is very different from squeue, this flag is not supported.

	return cmd
}

func scontrol() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "scontrol",
		Short:              "Wrapper of ccontrol command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			// Find the sub command
			firstSubCmd := ""
			for idx, arg := range args {
				if !strings.HasPrefix(arg, "-") {
					// Omit flags before the first subcommand
					firstSubCmd = arg
					args = args[idx+1:]
					break
				}
			}

			// Convert XXX=YYY into xxx YYY
			convertedArgs := make([]string, 0, len(args))
			re := regexp.MustCompile(`(?i)(\w+)=(.+)`)
			for _, arg := range args {
				if re.MatchString(arg) {
					matches := re.FindStringSubmatch(arg)
					// The regex must has 3 matches
					convertedArgs = append(convertedArgs, strings.ToLower(matches[1]), matches[2])
				} else {
					convertedArgs = append(convertedArgs, arg)
				}
			}

			switch firstSubCmd {
			case "show":
				// For `show`, do the keyword mapping:
				for idx, arg := range convertedArgs {
					switch strings.ToLower(arg) {
					case "jobid":
						convertedArgs[idx] = "job"
					}
				}
				convertedArgs = append([]string{"show"}, convertedArgs...)
			case "update":
				// For `update`, the mapping is more complex
				for idx, arg := range convertedArgs {
					switch strings.ToLower(arg) {
					case "jobid":
						convertedArgs[idx] = "job"
					case "nodename":
						convertedArgs[idx] = "node"
					case "partitionname":
						convertedArgs[idx] = "partition"
					case "timelimit":
						convertedArgs[idx] = "--time-limit"
					case "state":
						convertedArgs[idx] = "--state"
					case "reason":
						convertedArgs[idx] = "--reason"
					}
				}

				secondSubCmd := ""
				for idx, arg := range convertedArgs {
					switch arg {
					case "job":
						convertedArgs[idx] = "--job"
					case "node":
						secondSubCmd = "node"
						convertedArgs[idx] = "--name"
					}
				}

				if secondSubCmd == "" {
					convertedArgs = append([]string{"update"}, convertedArgs...)
				} else {
					convertedArgs = append([]string{"update", secondSubCmd}, convertedArgs...)
				}
			default:
				// If no subcommand is found, just fall back to ccontrol.
				log.Debug("Unknown subcommand: ", firstSubCmd)
			}

			log.Debug("Converted args: ", convertedArgs)

			// Find the matching subcommand
			subcmd, convertedArgs, err := ccontrol.RootCmd.Traverse(convertedArgs)
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}

			ccontrol.RootCmd.PersistentPreRun(cmd, convertedArgs)
			subcmd.InitDefaultHelpFlag()
			if err = subcmd.ParseFlags(convertedArgs); err != nil {
				log.Error(err)
				os.Exit(1)
			}
			convertedArgs = subcmd.Flags().Args()

			if subcmd.Runnable() {
				subcmd.Run(cmd, convertedArgs)
			} else {
				subcmd.Help()
			}
		},
	}

	return cmd
}
