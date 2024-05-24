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
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cwrapper",
		Short: "Wrapper of CraneSched commands",
		Long:  "",
	}
)

func ParseCmdArgs() {
	// Slurm Commands
	rootCmd.AddGroup(slurmGroup)
	{
		rootCmd.AddCommand(squeue())
		rootCmd.AddCommand(scontrol())
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
