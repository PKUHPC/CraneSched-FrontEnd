/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cwrapper

import (
	"CraneFrontEnd/internal/cacct"
	"CraneFrontEnd/internal/cbatch"
	"CraneFrontEnd/internal/ccancel"
	"CraneFrontEnd/internal/ccontrol"
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/util"
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

/*
Overall, we have two ways to implement the Wrapper.
- Directly use Cobra to reimplement a command, including all its subcommands and flags,
  then call the original command.
- Do not reimplement a new command, but directly call the original command after
  replacing and modifying the args.

For the first, since we have reimplemented a command, we get a realistic help and command line experience.
 However, if the wrapped command is too complex, the amount of code will be relatively large.

For the second, we directly call the original command, so the amount of code will be relatively small.
 However, it requires a series of processing on the args, which is likely to lead to some corner cases
 not being properly handled.

In this file, ccontrol is too complex, and sbatch and cbatch are almost the same, so we choose the 2nd approach.
The remaining commands are relatively simple, so we choose the first approach.
*/

var slurmGroup = &cobra.Group{
	ID:    "slurm",
	Title: "Slurm Commands:",
}

func sacct() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sacct",
		Short:   "Wrapper of cacct command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			cacct.RootCmd.PersistentPreRun(cmd, args)
			// Validate the arguments
			if err := cacct.RootCmd.ValidateArgs(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			cacct.RootCmd.Run(cmd, args)
		},
	}

	cmd.Flags().StringVarP(&cacct.FlagFilterAccounts, "account", "A", "",
		"Displays jobs when a comma separated list of accounts are given as the argument.")
	cmd.Flags().StringVarP(&cacct.FlagFilterJobIDs, "jobs", "j", "",
		"Displays information about the specified job or list of jobs.")
	cmd.Flags().StringVarP(&cacct.FlagFilterUsers, "user", "u", "",
		"Use this comma separated list of user names to select jobs to display.")

	cmd.Flags().StringVarP(&cacct.FlagFilterStartTime, "starttime", "S", "",
		"Select jobs in any state after the specified time.")
	cmd.Flags().StringVarP(&cacct.FlagFilterEndTime, "endtime", "E", "",
		"Select jobs in any state before the specified time.")

	cmd.Flags().BoolVarP(&cacct.FlagNoHeader, "noheader", "n", false,
		"No heading will be added to the output. The default action is to display a header.")

	return cmd
}

// func sacctmgr() *cobra.Command {

// }

func scancel() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "scancel",
		Short:   "Wrapper of ccancel command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			// scancel uses spaced arguments,
			// we need to convert it into a comma-separated list.
			if len(args) > 0 {
				args = []string{strings.Join(args, ",")}
			}

			ccancel.RootCmd.PersistentPreRun(cmd, args)
			if err := ccancel.RootCmd.ValidateArgs(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			ccancel.RootCmd.Run(cmd, args)
		},
	}

	cmd.Flags().BoolP("help", "", false, "Help for this command.")

	cmd.Flags().StringVarP(&ccancel.FlagAccount, "account", "A", "",
		"Restrict the scancel operation to jobs under this charge account.")
	cmd.Flags().StringVarP(&ccancel.FlagJobName, "name", "n", "",
		"Restrict the scancel operation to jobs with this job name.") // TODO: Alias --jobname
	cmd.Flags().StringSliceVarP(&ccancel.FlagNodes, "nodelist", "w", nil,
		"Cancel any jobs using any of the given hosts. (comma-separated list of hosts)") // TODO: Read from file
	cmd.Flags().StringVarP(&ccancel.FlagPartition, "partition", "p", "",
		"Restrict the scancel operation to jobs in this partition.")
	cmd.Flags().StringVarP(&ccancel.FlagState, "state", "t", "",
		`Restrict the scancel operation to jobs in this state.`) // TODO: Give hints on valid state strings
	cmd.Flags().StringVarP(&ccancel.FlagUserName, "user", "u", "",
		"Restrict the scancel operation to jobs owned by the given user.")

	return cmd
}

func sbatch() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "sbatch",
		Short:              "Wrapper of cbatch command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			// Parse flags
			cbatch.RootCmd.InitDefaultHelpFlag()
			if err := cbatch.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = cbatch.RootCmd.Flags().Args()
			if help, err := cbatch.RootCmd.Flags().GetBool("help"); err != nil || help {
				cbatch.RootCmd.Help()
				return
			}

			// Validate the arguments
			if err := cbatch.RootCmd.ValidateArgs(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			cbatch.RootCmd.Run(cmd, args)
		},
	}

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
				os.Exit(util.ErrorCmdArg)
			}

			ccontrol.RootCmd.PersistentPreRun(cmd, convertedArgs)
			subcmd.InitDefaultHelpFlag()
			if err = subcmd.ParseFlags(convertedArgs); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			convertedArgs = subcmd.Flags().Args()

			if subcmd.Runnable() {
				subcmd.Run(subcmd, convertedArgs)
			} else {
				subcmd.Help()
			}
		},
	}

	return cmd
}

func sinfo() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sinfo",
		Short:   "Wrapper of cinfo command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			cinfo.RootCmd.Run(cmd, args)
		},
	}

	cmd.Flags().BoolVarP(&cinfo.FlagSummarize, "summarize", "s", false,
		"List only a partition state summary with no node state details.")
	cmd.Flags().BoolVarP(&cinfo.FlagListReason, "list-reasons", "R", false,
		"List reasons nodes are in the down, drained, fail or failing state.")

	cmd.Flags().BoolVarP(&cinfo.FlagFilterDownOnly, "dead", "d", false,
		"If set, only report state information for non-responding (dead) nodes.")
	cmd.Flags().BoolVarP(&cinfo.FlagFilterRespondingOnly, "responding", "r", false,
		"If set only report state information for responding nodes.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterPartitions, "partition", "p", nil,
		"Print information about the node(s) in the specified partition(s). \nMultiple partitions are separated by commas.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterNodes, "nodes", "n", nil,
		"Print information about the specified node(s). \nMultiple nodes are separated by commas.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterCranedStates, "states", "t", nil,
		"List nodes only having the given state(s). Multiple states may be comma separated.")

	cmd.Flags().Uint64VarP(&cinfo.FlagIterate, "iterate", "i", 0,
		"Print the state on a periodic basis. Sleep for the indicated number of seconds between reports.")

	return cmd
}

func squeue() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "squeue",
		Short:   "Wrapper of cqueue command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			// Validate the arguments
			if err := cqueue.RootCmd.ValidateArgs(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			cqueue.RootCmd.Run(cmd, args)
		},
	}

	// As --noheader will use -h, we need to add the help flag manually
	cmd.Flags().BoolP("help", "", false, "Help for this command.")

	cmd.Flags().BoolVarP(&cqueue.FlagNoHeader, "noheader", "h", false,
		"Do not print a header on the output.")
	cmd.Flags().BoolVarP(&cqueue.FlagStartTime, "start", "S", false,
		"Report the expected start time and resources to be allocated for pending jobs in order of \nincreasing start time.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterPartitions, "partition", "p", "",
		"Specify the partitions of the jobs or steps to view. Accepts a comma separated list of \npartition names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobIDs, "jobs", "j", "",
		"Specify a comma separated list of job IDs to display. Defaults to all jobs. ")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobNames, "name", "n", "",
		"Request jobs or job steps having one of the specified names. The list consists of a comma \nseparated list of job names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterQos, "qos", "q", "",
		"Specify the qos(s) of the jobs or steps to view. Accepts a comma separated list of qos's.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterStates, "states", "t", "",
		"Specify the states of jobs to view. Accepts a comma separated list of state names or \"all\".")
	cmd.Flags().StringVarP(&cqueue.FlagFilterUsers, "user", "u", "",
		"Request jobs or job steps from a comma separated list of users.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterAccounts, "account", "A", "",
		"Specify the accounts of the jobs to view. Accepts a comma separated list of account names.")
	cmd.Flags().Uint64VarP(&cqueue.FlagIterate, "iterate", "i", 0,
		"Repeatedly gather and report the requested information at the interval specified (in seconds). \nBy default, prints a time stamp with the header.")

	// The following flags are not supported by the wrapper
	// --format, -o: As the cqueue's output format is very different from squeue, this flag is not supported.

	return cmd
}
