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
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"strings"

	"github.com/spf13/cobra"
)

var lsfGroup = &cobra.Group{
	ID:    "lsf",
	Title: "LSF Commands:",
}

var (
	FlagBsub_B  bool
	FlagBsub_N  bool
	FlagBsub_Ne bool
)

func bacct() *cobra.Command {
	// bacct: args represent job ids
	// cacct: no args
	cmd := &cobra.Command{
		Use:     "bacct [flags] [job_id ...]",
		Short:   "Wrapper of bacct command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cacct.RootCmd.PersistentPreRun(cmd, []string{})
			cacct.FlagFilterJobIDs = strings.Join(args, ",")
			cacct.RootCmd.Run(cmd, []string{})
		},
	}

	cmd.Flags().StringVar(&cacct.FlagFilterUsers, "u", "", "Displays accounting statistics for jobs that are submitted by the specified users (commas separated list)")
	cmd.Flags().StringVar(&cacct.FlagFilterStartTime, "D",
		"", "Displays accounting statistics for jobs that are dispatched during the specified time interval"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	cmd.Flags().StringVar(&cacct.FlagFilterEndTime, "C",
		"", "Displays accounting statistics for jobs that completed or exited during the specified time interval"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	cmd.Flags().StringVar(&cacct.FlagFilterSubmitTime, "S",
		"", "Displays accounting statistics for jobs that are submitted during the specified time interval"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	cmd.Flags().BoolVar(&cacct.FlagFull, "l", false, "Long format. Displays detailed information for each job in a multiline format")

	return cmd
}

func bsub() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bsub [flags] file",
		Short:   "Wrapper of bsub command",
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
			cbatch.RootCmd.Run(cmd, args)
		},
	}

	cmd.Flags().StringVar(&cbatch.FlagJob, "J", "", "Assigns the specified name to the job")
	cmd.Flags().StringVar(&cbatch.FlagStdoutPath, "o", "", "Appends the standard output of the job to the specified file path")
	cmd.Flags().StringVar(&cbatch.FlagStderrPath, "e", "", "Appends the standard error output of the job to the specified file path")
	cmd.Flags().Uint32Var(&cbatch.FlagNodes, "nnode", 1, "Specifies the number of compute nodes that are required for the job")
	cmd.Flags().Uint32Var(&cbatch.FlagNtasksPerNode, "n", 1, "Submits a parallel job and specifies the number of tasks in the job")
	cmd.Flags().StringVar(&cbatch.FlagTime, "W", "", "Sets the runtime limit of the job")
	cmd.Flags().StringVar(&cbatch.FlagMem, "M", "", "Sets a memory limit for all the processes that belong to the job")
	cmd.Flags().StringVar(&cbatch.FlagCwd, "cwd", "", "Specifies the current working directory for job execution")
	cmd.Flags().StringVar(&cbatch.FlagPartition, "q", "", "Submits the job to the specified queue (partition)") // lsf 支持同时指定多个 queue，实现待定
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
		Short:   "Wrapper of bjobs command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cqueue.FlagFilterJobIDs = strings.Join(args, ",")
			cqueue.RootCmd.Run(cmd, []string{})
		},
	}

	cmd.Flags().StringVar(&cqueue.FlagFormat, "o", "", "Sets the customized output format")
	cmd.Flags().StringVar(&cqueue.FlagFilterJobNames, "J", "", "Displays information about jobs with the specified job name")

	return cmd
}

func bqueues() *cobra.Command {
	// bqueues: args represent queue names
	// cinfo: no args
	cmd := &cobra.Command{
		Use:     "bqueues [flags] [queue_name ...]",
		Short:   "Wrapper of bqueues command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cinfo.FlagFilterPartitions = args
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
		Short:   "Wrapper of bkill command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				args = []string{strings.Join(args, ",")}
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
