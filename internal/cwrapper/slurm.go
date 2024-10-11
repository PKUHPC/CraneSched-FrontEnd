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
	"CraneFrontEnd/internal/cacctmgr"
	"CraneFrontEnd/internal/calloc"
	"CraneFrontEnd/internal/cbatch"
	"CraneFrontEnd/internal/ccancel"
	"CraneFrontEnd/internal/ccontrol"
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/crun"
	"CraneFrontEnd/internal/util"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

/*
We have two ways to implement the Wrapper:
- Directly use Cobra to reimplement a command, including all its subcommands and flags,
  then call the original command.
- Do not reimplement a new command, but directly call the original command after
  replacing and modifying the args.

For the first, since we have reimplemented a command, we get a realistic help and command line experience.
 However, if the wrapped command is too complex, the amount of code will be relatively large.

For the second, we directly call the original command, so the amount of code will be relatively small.
 However, it requires a series of processing on the args, which is likely to lead to some corner cases
 not being properly handled.

To sum up, ccontrol, cacctmgr, cbatch, calloc and crun are too complex or very same to their
 slurm counterparts, so we choose the 2nd way. The rest of the commands follow the 1st approach.
*/

type SlurmWrapper struct {
}

func (w SlurmWrapper) Group() *cobra.Group {
	return &cobra.Group{
		ID:    "slurm",
		Title: "Slurm Commands:",
	}
}

func (w SlurmWrapper) SubCommands() []*cobra.Command {
	return []*cobra.Command{
		sacct(),
		sacctmgr(),
		salloc(),
		sbatch(),
		scancel(),
		scontrol(),
		sinfo(),
		squeue(),
		srun(),
	}
}

func (w SlurmWrapper) HasCommand(cmd string) bool {
	return slices.Contains([]string{"sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "sinfo", "squeue"}, cmd)
}

func (w SlurmWrapper) Preprocess() error {
	// Slurm commands do not need any preprocessing for os.Args
	return nil
}

func sacct() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sacct",
		Short:   "Wrapper of cacct command",
		Long:    "",
		GroupID: "slurm",
		Run: func(cmd *cobra.Command, args []string) {
			cacct.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(cacct.RootCmd, args); err != nil {
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

func sacctmgr() *cobra.Command {
	entity := func(name string) bool {
		supported := []string{"account", "user", "qos"}
		for _, s := range supported {
			if name == s {
				return true
			}
		}
		return false
	}

	option := func(arg string) (string, error) {
		if arg == "name" || arg == "names" {
			return "--name", nil
		} else if arg == "account" || arg == "accounts" {
			return "--account", nil
		} else if arg == "partition" || arg == "partitions" {
			return "--partition", nil
		} else if arg == "parent" {
			return "--parent", nil
		} else if arg == "adminlevel" {
			return "--level", nil
		} else if arg == "description" {
			return "--description", nil
		} else if arg == "priority" {
			return "--priority", nil
		} else if arg == "maxjobpu" || arg == "maxjobsperuser" {
			return "--max-jobs-per-user", nil
		} else if arg == "maxcpupu" || arg == "maxcpuperuser" {
			return "--max-cpus-per-user", nil
		} else if arg == "maxwall" || arg == "maxwalldurationperjob" {
			return "--max-time-limit-per-task", nil
		} else {
			return arg, errors.New("Unsupported arguments: " + arg)
		}
	}

	cmd := &cobra.Command{
		Use:                "sacctmgr",
		Short:              "Wrapper of cacctmgr command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			// As sacctmgr only has flags on RootCmd,
			// we collect all flags first
			flags := make([]string, 0)
			convertedArgs := make([]string, 0)
			for _, arg := range args {
				if arg == "--help" || arg == "-h" {
					fmt.Println("Please refer to the user manual of Slurm.")
					return
				} else if strings.HasPrefix(arg, "-") {
					flags = append(flags, arg)
				} else {
					convertedArgs = append(convertedArgs, arg)
				}
			}

			// Check length and entities
			if len(convertedArgs) < 2 {
				log.Error("cacctmgr requires at least 2 arguments")
				os.Exit(util.ErrorCmdArg)
			}

			// Keyword mapping for subcommands
			switch convertedArgs[0] {
			case "create":
				convertedArgs[0] = "add"
			case "remove":
				convertedArgs[0] = "delete"
			case "update":
				convertedArgs[0] = "modify"
			case "list":
				convertedArgs[0] = "show"
			}

			// Check entities
			if !entity(strings.ToLower(convertedArgs[1])) {
				log.Error("cacctmgr unsupported entity: ", convertedArgs[1])
				os.Exit(util.ErrorCmdArg)
			}

			// Convert XXX=YYY into xxx=YYY
			// If XXX is missing, use `name`
			re := regexp.MustCompile(`(?i)^(\w+)=(.+)`)
			for i := 2; i < len(convertedArgs); i++ {
				// Ignore `where` and `set` options
				arg := convertedArgs[i]
				if arg == "where" || arg == "set" {
					continue
				}

				if re.MatchString(arg) {
					matches := re.FindStringSubmatch(arg)
					// The regex must has 3 matches
					arg = strings.ToLower(matches[1]) + "=" + matches[2]
				} else {
					arg = "name=" + arg
				}

				convertedArgs[i] = arg
			}

			// Handle the subcommands
			if convertedArgs[0] == "add" || convertedArgs[0] == "delete" {
				// For `add` and `delete`, there is no `where` or `set` options
				var err error
				for i := 2; i < len(convertedArgs); i++ {
					if convertedArgs[i] == "where" || convertedArgs[i] == "set" {
						log.Error("cacctmgr add/delete does not support where/set")
						os.Exit(util.ErrorCmdArg)
					}

					if strings.Contains(convertedArgs[i], ",") {
						log.Error("cacctmgr add/delete allows only one entity at a time")
						os.Exit(util.ErrorCmdArg)
					}

					l := strings.Split(convertedArgs[i], "=")
					if len(l) != 2 {
						log.Error("Invalid argument: ", convertedArgs[i])
						os.Exit(util.ErrorCmdArg)
					}

					l[0], err = option(l[0])
					if err != nil {
						log.Error(err)
						os.Exit(util.ErrorCmdArg)
					}

					convertedArgs[i] = l[0] + "=" + l[1]
				}
			} else if convertedArgs[0] == "modify" {
				// For `modify`, there is `where` and `set`
				// Find all `where` and `set` options
				whereMap := make(map[string]string)
				setMap := make(map[string]string)
				mode := "where"
				for i := 2; i < len(convertedArgs); i++ {
					if strings.Contains(convertedArgs[i], ",") {
						log.Error("cacctmgr modify allows only one entity at a time")
						os.Exit(util.ErrorCmdArg)
					}

					l := strings.Split(convertedArgs[i], "=")
					if len(l) != 2 {
						if convertedArgs[i] == "where" || convertedArgs[i] == "set" {
							mode = convertedArgs[i]
							continue
						}
						log.Error("Invalid argument: ", convertedArgs[i])
						os.Exit(util.ErrorCmdArg)
					}

					if mode == "where" {
						whereMap[l[0]] = l[1]
					} else {
						setMap[l[0]] = l[1]
					}
				}
				convertedArgs = convertedArgs[:2]

				if len(whereMap) == 0 || len(setMap) == 0 {
					log.Error("Modify requires both where and set conditions")
					os.Exit(util.ErrorCmdArg)
				}

				// Check where condition
				for k, v := range whereMap {
					if k != "name" && k != "names" {
						log.Error("Modify only supports where name=XXX")
						os.Exit(util.ErrorCmdArg)
					}
					convertedArgs = append(convertedArgs, "--name="+v)
				}

				// Check set condition
				for k, v := range setMap {
					if nk, err := option(k); err != nil {
						log.Error(err)
						os.Exit(util.ErrorCmdArg)
					} else {
						convertedArgs = append(convertedArgs, nk, v)
					}
				}
			} else if convertedArgs[0] == "show" {
				// For `list`, there is `where` but no `set`
				whereMap := make(map[string]string)
				for i := 2; i < len(convertedArgs); i++ {
					if strings.Contains(convertedArgs[i], ",") {
						log.Error("cacctmgr show allows only one entity at a time")
						os.Exit(util.ErrorCmdArg)
					}

					l := strings.Split(convertedArgs[i], "=")
					if len(l) != 2 {
						if convertedArgs[i] == "where" {
							continue
						}
						log.Error("Invalid argument: ", convertedArgs[i])
						os.Exit(util.ErrorCmdArg)
					}

					whereMap[l[0]] = l[1]
				}

				convertedArgs = convertedArgs[:2]
				for k, v := range whereMap {
					if k == "name" || k == "names" {
						convertedArgs = append(convertedArgs, v)
					} else if k == "account" || k == "accounts" {
						convertedArgs = append(convertedArgs, "--account="+v)
					} else {
						log.Error("Unsupported arguments: ", k)
						os.Exit(util.ErrorCmdArg)
					}
				}
			} else {
				log.Error("Unsupported subcommand: ", convertedArgs[0])
				os.Exit(util.ErrorCmdArg)
			}

			// Add other flags
			convertedArgs = append(convertedArgs, flags...)

			// Find the matching subcommand
			subcmd, convertedArgs, err := cacctmgr.RootCmd.Traverse(convertedArgs)
			if err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			// Parse the flags
			cacctmgr.RootCmd.PersistentPreRun(cmd, convertedArgs)
			subcmd.InitDefaultHelpFlag()
			if err = subcmd.ParseFlags(convertedArgs); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			convertedArgs = subcmd.Flags().Args()

			// Validate the arguments and flags
			if err := Validate(subcmd, convertedArgs); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			if subcmd.Runnable() {
				subcmd.Run(subcmd, convertedArgs)
			} else {
				subcmd.Help()
			}
		},
	}

	return cmd
}

func salloc() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "salloc",
		Short:              "Wrapper of calloc command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			// Add --help from calloc
			calloc.RootCmd.InitDefaultHelpFlag()

			// Parse flags
			if err := calloc.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = calloc.RootCmd.Flags().Args()
			if help, err := calloc.RootCmd.Flags().GetBool("help"); err != nil || help {
				calloc.RootCmd.Help()
				return
			}

			calloc.RootCmd.PersistentPreRun(cmd, args)
			// Validate the arguments
			if err := Validate(calloc.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			calloc.RootCmd.Run(calloc.RootCmd, args)
		},
	}

	return cmd
}

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
			if err := Validate(ccancel.RootCmd, args); err != nil {
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

			cbatch.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(cbatch.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			cbatch.RootCmd.Run(cbatch.RootCmd, args)
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
			re := regexp.MustCompile(`(?i)^(\w+)=(.+)`)
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
						secondSubCmd = "job"
						convertedArgs[idx] = "--job"
					case "node":
						secondSubCmd = "node"
						convertedArgs[idx] = "--name"
					}
				}

				if secondSubCmd == "" {
					log.Error("Only \"job\" and \"node\" could be updated.")
					os.Exit(util.ErrorCmdArg)
				}
				convertedArgs = append([]string{"update", secondSubCmd}, convertedArgs...)
			case "hold":
				concatedTaskIds := ""
				for i := 0; i < len(convertedArgs); i++ {
					if convertedArgs[i] != "job" && convertedArgs[i] != "jobid" {
						// If not "job" or "jobid", it should be a job id list
						// Trim is needed as slurm supports "hold ,1,2,3" but crane doesn't
						convertedArgs[i] = strings.Trim(convertedArgs[i], ",")
						_, err := util.ParseJobIdList(convertedArgs[i], ",")
						if err != nil {
							log.Errorln(err)
							os.Exit(util.ErrorCmdArg)
						}
						concatedTaskIds += "," + convertedArgs[i]
					}
				}
				convertedArgs = append([]string{"hold"}, strings.Trim(concatedTaskIds, ","))
			case "release":
				concatedTaskIds := ""
				for i := 0; i < len(convertedArgs); i++ {
					if convertedArgs[i] != "job" && convertedArgs[i] != "jobid" {
						// If not "job" or "jobid", it should be a job id list
						// Trim is needed as slurm supports "release ,1,2,3" but crane doesn't
						convertedArgs[i] = strings.Trim(convertedArgs[i], ",")
						_, err := util.ParseJobIdList(convertedArgs[i], ",")
						if err != nil {
							log.Errorln(err)
							os.Exit(util.ErrorCmdArg)
						}
						concatedTaskIds += "," + convertedArgs[i]
					}
				}
				convertedArgs = append([]string{"release"}, strings.Trim(concatedTaskIds, ","))
			default:
				// If no subcommand is found, just fall back to ccontrol.
				log.Debug("Unknown subcommand: ", firstSubCmd)
			}

			// Find the matching subcommand
			subcmd, convertedArgs, err := ccontrol.RootCmd.Traverse(convertedArgs)
			if err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			// Parse the flags
			ccontrol.RootCmd.PersistentPreRun(cmd, convertedArgs)
			subcmd.InitDefaultHelpFlag()
			if err = subcmd.ParseFlags(convertedArgs); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			convertedArgs = subcmd.Flags().Args()

			// Validate the arguments and flags
			if err := Validate(subcmd, convertedArgs); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

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
			cinfo.RootCmd.PersistentPreRun(cmd, args)
			cinfo.RootCmd.Run(cmd, args)
		},
	}

	// cmd.Flags().BoolVarP(&cinfo.FlagSummarize, "summarize", "s", false,
	// 	"List only a partition state summary with no node state details.")
	// cmd.Flags().BoolVarP(&cinfo.FlagListReason, "list-reasons", "R", false,
	// 	"List reasons nodes are in the down, drained, fail or failing state.")

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
			cqueue.RootCmd.PersistentPreRun(cmd, args)
			// Validate the arguments
			if err := Validate(cqueue.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			if !cqueue.FlagNoHeader {
				fmt.Printf("%s %s %s %s %s %s %s %s\n",
					"JOBID", "PARTITION", "NAME", "USER", "ST", "TIME", "NODES", "NODELIST(REASON)")
				cqueue.FlagNoHeader = true
			}
			cqueue.FlagFormat = "%j %P %n %u %t %e %N %r"
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

func srun() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "srun",
		Short:              "Wrapper of crun command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			crun.RootCmd.InitDefaultHelpFlag()

			if err := crun.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = crun.RootCmd.Flags().Args()
			if help, err := crun.RootCmd.Flags().GetBool("help"); err != nil || help {
				crun.RootCmd.Help()
				return
			}

			crun.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(crun.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			crun.RootCmd.Run(crun.RootCmd, args)
		},
	}

	return cmd
}

func Validate(c *cobra.Command, args []string) error {
	if err := c.ValidateArgs(args); err != nil {
		return err
	}
	if err := c.ValidateRequiredFlags(); err != nil {
		return err
	}
	if err := c.ValidateFlagGroups(); err != nil {
		return err
	}
	return nil
}
