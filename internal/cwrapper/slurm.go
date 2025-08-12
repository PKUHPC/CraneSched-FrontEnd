/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package cwrapper

import (
	"CraneFrontEnd/generated/protos"
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
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			cacct.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(cacct.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			return cacct.RootCmd.RunE(cmd, args)
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
	cmd := &cobra.Command{
		Use:                "sacctmgr",
		Short:              "Wrapper of cacctmgr command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			convertedArgs := make([]string, 0, len(args)+1)
			convertedArgs = append(convertedArgs, "cacctmgr")

			// Handle help
			for _, arg := range args {
				if arg == "--help" || arg == "-h" {
					fmt.Println("Please refer to the user manual of Slurm.")
					return nil
				}
			}

			// Process each argument
			for _, arg := range args {
				switch arg {
				// Map slurm commands to crane commands
				case "create":
					convertedArgs = append(convertedArgs, "add")
				case "remove":
					convertedArgs = append(convertedArgs, "delete")
				case "update":
					convertedArgs = append(convertedArgs, "modify")
				case "list":
					convertedArgs = append(convertedArgs, "show")
				// Convert option names to lowercase for consistency
				default:
					if strings.Contains(arg, "=") {
						parts := strings.SplitN(arg, "=", 2)
						key := strings.ToLower(parts[0])
						value := parts[1]
						
						switch key {
						case "names":
							key = "name"
						case "accounts":
							key = "account"
						case "partitions":
							key = "partition"
						case "adminlevel":
							key = "adminlevel"
						case "maxjobpu", "maxjobsperuser":
							key = "maxjobsperuser"
						case "maxcpupu", "maxcpuperuser":
							key = "maxcpusperuser"
						case "maxwall", "maxwalldurationperjob":
							key = "maxtimelimitpertask"
						case "maxsubmitjobsperuser":
							key = "maxjobsperuser"
						}
						
						convertedArgs = append(convertedArgs, key+"="+value)
					} else {
						// Keep other arguments as-is (like "where", "set", entity names, etc.)
						convertedArgs = append(convertedArgs, strings.ToLower(arg))
					}
				}
			}

			// Call cacctmgr with converted arguments
			cacctmgr.ParseCmdArgs(convertedArgs)
			return nil
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
		RunE: func(cmd *cobra.Command, args []string) error {
			// Add --help from calloc
			calloc.RootCmd.InitDefaultHelpFlag()

			// Parse flags
			if err := calloc.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = calloc.RootCmd.Flags().Args()
			if help, err := calloc.RootCmd.Flags().GetBool("help"); err != nil || help {
				return calloc.RootCmd.Help()
			}

			calloc.RootCmd.PersistentPreRun(cmd, args)
			// Validate the arguments
			if err := Validate(calloc.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			return calloc.RootCmd.RunE(calloc.RootCmd, args)
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
		RunE: func(cmd *cobra.Command, args []string) error {
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
			return ccancel.RootCmd.RunE(cmd, args)
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
		RunE: func(cmd *cobra.Command, args []string) error {
			cbatch.RootCmd.InitDefaultHelpFlag()

			if err := cbatch.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = cbatch.RootCmd.Flags().Args()
			if help, err := cbatch.RootCmd.Flags().GetBool("help"); err != nil || help {
				return cbatch.RootCmd.Help()
			}

			cbatch.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(cbatch.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			return cbatch.RootCmd.RunE(cbatch.RootCmd, args)
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
		RunE: func(cmd *cobra.Command, args []string) error {
			// Find the sub command
			firstSubCmd := ""
			leadingFlags := make([]string, 0)
			for idx, arg := range args {
				if !strings.HasPrefix(arg, "-") {
					// Omit flags before the first subcommand
					firstSubCmd = arg
					args = args[idx+1:]
					break
				}
				leadingFlags = append(leadingFlags, arg)
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
						if _, err := util.ParseJobIdList(convertedArgs[i], ","); err != nil {
							log.Errorf("Invalid job list specified: %v.\n", err)
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
						if _, err := util.ParseJobIdList(convertedArgs[i], ","); err != nil {
							log.Errorf("Invalid job list specified: %v.\n", err)
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

			// use ccontrol to parse the arguments
			allArgs := append([]string{"ccontrol"}, append(leadingFlags, convertedArgs...)...)
			ccontrol.ParseCmdArgs(allArgs)
			return cmd.Help()
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
		RunE: func(cmd *cobra.Command, args []string) error {
			cinfo.RootCmd.PersistentPreRun(cmd, args)
			return cinfo.RootCmd.RunE(cmd, args)
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

			err := util.ErrorSuccess
			if cqueue.FlagIterate != 0 {
				err = squeueLoopedQuery(cqueue.FlagIterate)
			} else {
				err = squeueQuery()
			}
			os.Exit(err)
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

func squeueQueryTableOutput(reply *protos.QueryTasksInfoReply) util.CraneCmdError {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"JOBID", "PARTITION", "NAME", "USER",
		"ST", "TIME", "NODES", "NODELIST(REASON)"}
	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		var timeElapsedStr string
		if reply.TaskInfoList[i].Status == protos.TaskStatus_Running {
			timeElapsedStr = util.SecondTimeFormat(reply.TaskInfoList[i].ElapsedTime.Seconds)
		} else {
			timeElapsedStr = "-"
		}

		var reasonOrListStr string
		if reply.TaskInfoList[i].Status == protos.TaskStatus_Pending {
			reasonOrListStr = reply.TaskInfoList[i].GetPendingReason()
		} else {
			reasonOrListStr = reply.TaskInfoList[i].GetCranedList()
		}

		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Username,
			reply.TaskInfoList[i].Status.String(),
			timeElapsedStr,
			strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10),
			reasonOrListStr,
		}
	}

	if cqueue.FlagStartTime {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			startTime := reply.TaskInfoList[i].StartTime
			if startTime.Seconds != 0 {
				tableData[i] = append(tableData[i],
					startTime.AsTime().In(time.Local).
						Format("2006-01-02 15:04:05"))
			} else {
				tableData[i] = append(tableData[i], "")
			}
		}
	}
	if cqueue.FlagFilterQos != "" {
		header = append(header, "QoS")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].Qos)
		}
	}

	if !cqueue.FlagNoHeader {
		table.SetHeader(header)
	}

	table.AppendBulk(tableData)
	table.Render()
	return util.ErrorSuccess
}

func squeueQuery() util.CraneCmdError {
	reply, err := cqueue.QueryTasksInfo()
	if err != nil {
		var craneErr *util.CraneError
		if errors.As(err, &craneErr) {
			return craneErr.Code
		} else {
			log.Errorf("Unknown error occurred: %s.", err)
			return util.ErrorGeneric
		}
	}

	return squeueQueryTableOutput(reply)
}

func squeueLoopedQuery(iterate uint64) util.CraneCmdError {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		log.Errorf("Invalid time interval: %v.\n", err)
		return util.ErrorCmdArg
	}
	for {
		fmt.Println(time.Now().String()[0:19])
		err := squeueQuery()
		if err != util.ErrorSuccess {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}

func srun() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "srun",
		Short:              "Wrapper of crun command",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			crun.RootCmd.InitDefaultHelpFlag()

			if err := crun.RootCmd.ParseFlags(args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}
			args = crun.RootCmd.Flags().Args()
			if help, err := crun.RootCmd.Flags().GetBool("help"); err != nil || help {
				return crun.RootCmd.Help()
			}

			crun.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(crun.RootCmd, args); err != nil {
				log.Error(err)
				os.Exit(util.ErrorCmdArg)
			}

			return crun.RootCmd.RunE(crun.RootCmd, args)
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
