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
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cwrapper",
		Short: "Wrapper of CraneSched commands",
		Long: `Wrapper of CraneSched commands.
This is a highly EXPERIMENTAL feature. 
If any error occurs, please refer to original commands.`,
	}
)

func ParseCmdArgs() {
	// Slurm Commands
	rootCmd.AddGroup(slurmGroup)
	{
		rootCmd.AddCommand(sacct())
		rootCmd.AddCommand(sacctmgr())
		rootCmd.AddCommand(sbatch())
		rootCmd.AddCommand(scancel())
		rootCmd.AddCommand(scontrol())
		rootCmd.AddCommand(sinfo())
		rootCmd.AddCommand(squeue())
	}

	// LSF Commands
	rootCmd.AddGroup(lsfGroup)
	{
		rootCmd.AddCommand(bacct())
		rootCmd.AddCommand(bsub())
		rootCmd.AddCommand(bjobs())
		rootCmd.AddCommand(bqueues())
		rootCmd.AddCommand(bkill())
	}

	/*
		LSF recognizes single dash option only. Whereas the current CLI library
		we are using does not support defining a shorthand-only flag, we
		used a detour approach to imitate that behavior.

		Check the subcommand name in `os.Args` before execute, if the name starts
		with "b", deem it's a LSF command, and preprocess the arguments,
		changing all single dash options to double dash.

		This approach still has several issues:
		- the options in command help info remain in double dash form
		- the criteria of judging if a command belongs to LSF is unreliable,
			considering more commands may be added in future

		Need a thorough refactor later.
	*/
	if os.Args[1][0] == 'b' {
		for i, v := range os.Args {
			// skip program name and subcommand
			if i <= 1 {
				continue
			}
			if v == "--" {
				break
			} else if len(v) >= 2 && v[0] == '-' && v[1] != '-' {
				os.Args[i] = "-" + v
			}
		}
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
