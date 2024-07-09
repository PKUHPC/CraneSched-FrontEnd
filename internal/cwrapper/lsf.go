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
	"strings"

	"github.com/spf13/cobra"
)

var lsfGroup = &cobra.Group{
	ID:    "lsf",
	Title: "LSF Commands:",
}

func bacct() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bacct",
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

	cmd.Flags().StringVarP(&cacct.FlagFilterStartTime, "start-time", "D", "", "...")
	cmd.Flags().StringVarP(&cacct.FlagFilterEndTime, "end-time", "C", "", "...")
	cmd.Flags().StringVarP(&cacct.FlagFilterSubmitTime, "submit-time", "S", "", "...")

	return cmd
}

func bsub() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bsub",
		Short:   "Wrapper of bsub command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	cmd.Flags().StringVarP(&cbatch.FlagJob, "job-name", "J", "", "...")
	cmd.Flags().StringVarP(&cbatch.FlagStdoutPath, "output", "o", "", "...")
	cmd.Flags().StringVarP(&cbatch.FlagStderrPath, "error", "e", "", "...")
	cmd.Flags().StringVarP(&cbatch.FlagMailUser, "mail-user", "u", "", "...")

	return cmd
}

func bjobs() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bjobs",
		Short:   "Wrapper of bjobs command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	return cmd
}

func bqueues() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bqueues",
		Short:   "Wrapper of bqueues command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	return cmd
}

func bkill() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bkill",
		Short:   "Wrapper of bkill command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	return cmd
}
