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
	"CraneFrontEnd/internal/cacct"
	"CraneFrontEnd/internal/cbatch"
	"CraneFrontEnd/internal/ccancel"
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type LSFWrapper struct {
}

func (w LSFWrapper) Group() *cobra.Group {
	return &cobra.Group{
		ID:    "lsf",
		Title: "LSF Commands:",
	}
}

func (w LSFWrapper) SubCommands() []*cobra.Command {
	return []*cobra.Command{
		bacct(),
		bsub(),
		bjobs(),
		bqueues(),
		bkill(),
	}
}

func (lsf LSFWrapper) HasCommand(cmd string) bool {
	return slices.Contains([]string{"bacct", "bsub", "bjobs", "bqueues", "bkill"}, cmd)
}

func (lsf LSFWrapper) Preprocess() error {
	/*
		LSF recognizes single dash option only. Whereas the CLI library cobra
		does not support defining something like `-env`.

		We imitate that behavior by preprocess the `os.Args`, changing all
		single dash options to double dash.
	*/
	for i, v := range os.Args {
		// Skip program name and subcommand
		if i <= 1 {
			continue
		}
		if v == "--" || v == "-h" {
			break
		} else if len(v) >= 2 && v[0] == '-' && v[1] != '-' {
			os.Args[i] = "-" + v
		}
	}

	return nil
}

var (
	FlagBacct_d bool
	FlagBacct_e bool

	FlagBsub_B  bool
	FlagBsub_N  bool
	FlagBsub_Ne bool
)

func bacct() *cobra.Command {
	// bacct: args represent job ids
	// cacct: no args
	cmd := &cobra.Command{
		Use:     "bacct [flags] [job_id ...]",
		Short:   "Wrapper of cacct command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cacct.FlagFilterUsers = strings.ReplaceAll(cacct.FlagFilterUsers, " ", ",")
			cacct.FlagFilterStartTime = ConvertInterval(cacct.FlagFilterStartTime)
			cacct.FlagFilterEndTime = ConvertInterval(cacct.FlagFilterEndTime)
			cacct.FlagFilterSubmitTime = ConvertInterval(cacct.FlagFilterSubmitTime)
			{
				states := []string{}
				if FlagBacct_d {
					states = append(states, "completed")
				}
				if FlagBacct_e {
					states = append(states, "failed", "cancelled", "time-limit-exceeded")
				}
				if len(states) > 0 {
					cacct.FlagFilterStates = strings.Join(states, ",")
				}
			}
			if len(args) == 1 && args[0] == "0" {
				cacct.FlagFilterJobIDs = ""
			} else if len(args) > 0 {
				cacct.FlagFilterJobIDs = strings.Join(args, ",")
			}

			cacct.RootCmd.PersistentPreRun(cmd, []string{})
			cacct.RootCmd.Run(cmd, []string{})
		},
	}

	cmd.Flags().StringVar(&cacct.FlagFilterUsers, "u", "", "Displays accounting statistics for jobs that are submitted by the specified users")
	cmd.Flags().StringVar(&cacct.FlagFilterStartTime, "D", "", "Displays accounting statistics for jobs that are dispatched during the specified time interval")
	cmd.Flags().StringVar(&cacct.FlagFilterEndTime, "C", "", "Displays accounting statistics for jobs that completed or exited during the specified time interval")
	cmd.Flags().StringVar(&cacct.FlagFilterSubmitTime, "S", "", "Displays accounting statistics for jobs that are submitted during the specified time interval")
	cmd.Flags().BoolVar(&cacct.FlagFull, "l", false, "Long format. Displays detailed information for each job in a multiline format")
	cmd.Flags().BoolVar(&FlagBacct_d, "d", false, "Displays accounting statistics for successfully completed jobs")
	cmd.Flags().BoolVar(&FlagBacct_e, "e", false, "Displays accounting statistics for exited jobs")
	cmd.Flags().BoolVar(&cacct.FlagNoHeader, "noheader", false, "Removes the column headings from the output")

	return cmd
}

func bsub() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bsub [flags] file",
		Short:   "Wrapper of cbatch command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			{
				mailTypes := make([]string, 0)
				if FlagBsub_B {
					mailTypes = append(mailTypes, "BEGIN")
				}
				if FlagBsub_N {
					mailTypes = append(mailTypes, "END")
				}
				if FlagBsub_Ne {
					mailTypes = append(mailTypes, "FAIL")
				}
				cbatch.FlagMailType = strings.Join(mailTypes, ",")
			}
			if cbatch.FlagNodelist != "" {
				cbatch.FlagNodelist = strings.ReplaceAll(cbatch.FlagNodelist, " ", ",")
			}
			cbatch.FlagTime = cbatch.ConvertLSFRuntimeLimit(cbatch.FlagTime)

			// Cbatch use Flags().Changed() to detect if some flag is set by user or default value.
			// Such flags(nodes, cpus-per-task, ntasks-per-node) need to be set if changed.
			if cmd.Flags().Changed("nnode") {
				cbatch.RootCmd.ParseFlags([]string{"--nodes", strconv.Itoa(int(cbatch.FlagNodes))})
			}
			if cmd.Flags().Changed("n") {
				cbatch.RootCmd.ParseFlags([]string{"--ntasks-per-node", strconv.Itoa(int(cbatch.FlagNtasksPerNode))})
			}

			cbatch.RootCmd.PersistentPreRun(cmd, args)
			cbatch.RootCmd.Run(cbatch.RootCmd, args)
		},
	}

	cmd.Flags().StringVar(&cbatch.FlagJob, "J", "", "Assigns the specified name to the job")
	cmd.Flags().StringVar(&cbatch.FlagStdoutPath, "o", "", "Appends the standard output of the job to the specified file path")
	cmd.Flags().StringVar(&cbatch.FlagStderrPath, "e", "", "Appends the standard error output of the job to the specified file path")
	cmd.Flags().Uint32Var(&cbatch.FlagNodes, "nnode", 1, "Specifies the number of compute nodes that are required for the job")
	cmd.Flags().Uint32Var(&cbatch.FlagNtasksPerNode, "n", 1, "Submits a parallel job and specifies the number of tasks in the job")
	cmd.Flags().StringVar(&cbatch.FlagTime, "W", "", "Sets the runtime limit of the job (timeFormat: [hour:]minute)")
	cmd.Flags().StringVar(&cbatch.FlagMem, "M", "", "Sets a memory limit for all the processes that belong to the job")
	cmd.Flags().StringVar(&cbatch.FlagCwd, "cwd", "", "Specifies the current working directory for job execution")
	cmd.Flags().StringVar(&cbatch.FlagPartition, "q", "", "Submits the job to the specified queue (partition)")
	cmd.Flags().StringVar(&cbatch.FlagNodelist, "m", "", "Submits a job to be run on specific host")
	cmd.Flags().StringVar(&cbatch.FlagExport, "env", "", "Controls the propagation of the specified job submission environment variables to the execution hosts")
	cmd.Flags().StringVar(&cbatch.FlagMailUser, "u", "", "Sends mail to the specified email destination")
	cmd.Flags().BoolVar(&FlagBsub_B, "B", false, "Sends mail to you when the job is dispatched and begins execution")
	cmd.Flags().BoolVar(&FlagBsub_N, "N", false, "Sends the job report to you by mail when the job finishes")
	cmd.Flags().BoolVar(&FlagBsub_Ne, "Ne", false, "Sends the job report to you by mail when the job failed")

	return cmd
}

func bjobs() *cobra.Command {
	// bjobs: args represent job ids
	// cqueue: no args
	cmd := &cobra.Command{
		Use:     "bjobs [flags] [job_id ...]",
		Short:   "Wrapper of cqueue command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cqueue.FlagFilterJobIDs = strings.Join(args, ",")
			cqueue.RootCmd.PersistentPreRun(cmd, []string{})
			if !cqueue.FlagNoHeader {
				fmt.Printf("%s %s %s %s %s %s %s %s\n",
					"JOBID", "USER", "STAT", "QUEUE", "FROM_HOST", "EXEC_HOST", "JOB_NAME", "SUBMIT_TIME")
				cqueue.FlagNoHeader = true
			}
			cqueue.FlagFormat = "%j %u %t %P %L %L %n %s"
			cqueue.RootCmd.Run(cmd, []string{})
		},
	}

	cmd.Flags().StringVar(&cqueue.FlagFilterJobNames, "J", "", "Displays information about jobs with the specified job name")
	cmd.Flags().BoolVar(&cqueue.FlagNoHeader, "noheader", false, "Removes the column headings from the output")

	return cmd
}

func bqueues() *cobra.Command {
	// bqueues: args represent queue names
	// cinfo: no args
	cmd := &cobra.Command{
		Use:     "bqueues [flags] [queue_name ...]",
		Short:   "Wrapper of cinfo command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cinfo.FlagFilterPartitions = args
			cinfo.RootCmd.PersistentPreRun(cmd, []string{})
			cinfo.RootCmd.Run(cmd, []string{})
		},
	}

	cmd.Flags().StringSliceVar(&cinfo.FlagFilterNodes, "m", nil, "Displays the queues that can run jobs on the specified host")

	return cmd
}

func bkill() *cobra.Command {
	// bkill: args represent job ids
	// ccancel: 1 arg, format "job_id1,job_id2,.."
	cmd := &cobra.Command{
		Use:     "bkill [flags] [job_id ...]",
		Short:   "Wrapper of ccancel command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			ignore_args, ignore_flags := false, false
			if ccancel.FlagState != "" {
				ignore_args = true
				ignore_flags = true
			}
			if len(args) == 1 && args[0] == "0" {
				ignore_args = true
			} else if len(args) > 0 {
				ignore_flags = true
			}

			if ignore_args {
				args = nil
			} else if len(args) > 0 {
				args = []string{strings.Join(args, ",")}
			}

			if ignore_flags {
				ccancel.FlagJobName = ""
				ccancel.FlagUserName = ""
				ccancel.FlagPartition = ""
				ccancel.FlagNodes = nil
			}

			ccancel.RootCmd.PersistentPreRun(cmd, args)
			ccancel.RootCmd.Run(cmd, args)
		},
	}

	cmd.Flags().StringVar(&ccancel.FlagJobName, "J", "", "Operates only on jobs with the specified job name")
	cmd.Flags().StringVar(&ccancel.FlagUserName, "u", "", "Operates only on jobs that are submitted by the specified user")
	cmd.Flags().StringVar(&ccancel.FlagPartition, "q", "", "Operates only on jobs in the specified queue (partition)")
	cmd.Flags().StringSliceVar(&ccancel.FlagNodes, "m", nil, "Operates only on jobs that are dispatched to the specified host")
	cmd.Flags().StringVar(&ccancel.FlagState, "stat", "", "Operates only on jobs in the specified status")

	return cmd
}

func ConvertInterval(t string) string {
	if t == "" {
		return t
	}
	ts := strings.Split(t, ",")
	if len(ts) == 1 {
		log.Fatal("Invalid LSF time format\n")
	}
	t1, t2 := ts[0], ts[1]
	t1, err1 := ConvertTime(t1, "left")
	if err1 != nil {
		log.Fatalf("Failed to parse LSF time format: %s\n", err1)
	}
	t2, err2 := ConvertTime(t2, "right")
	if err2 != nil {
		log.Fatalf("Failed to parse LSF time format: %s\n", err2)
	}
	return t1 + "~" + t2
}

func ConvertTime(t string, side string) (string, error) {
	curTime := time.Now()
	if t == "" || t == "." {
		return "", nil
	}
	// [year/][month/][day][/hour:minute|/hour:]
	re := regexp.MustCompile(`(?:(\d{4})/)?(?:(\d+)/)?(\d+)?(?:/(\d+)\:(\d+)?)?`)
	x := re.FindStringSubmatch(t)
	if x[0] != t {
		return t, errors.New("invalid LSF time format: " + t)
	}
	y, m, d, H, M := x[1], x[2], x[3], x[4], x[5]
	var S string
	if side == "left" {
		S = "00"
	} else if side == "right" {
		S = "59"
	} else {
		return t, errors.New("invalid side")
	}
	if y == "" {
		y = strconv.Itoa(curTime.Year())
	}
	if m == "" {
		m = strconv.Itoa(int(curTime.Month()))
	}
	if d == "" {
		d = strconv.Itoa(curTime.Day())
	}
	if H == "" {
		if side == "left" {
			H = "00"
		} else if side == "right" {
			H = "23"
		}
	}
	if M == "" {
		if side == "left" {
			M = "00"
		} else if side == "right" {
			M = "59"
		}
	}
	return fmt.Sprintf("%04s-%02s-%02sT%02s:%02s:%s", y, m, d, H, M, S), nil
}
