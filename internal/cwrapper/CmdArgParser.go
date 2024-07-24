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

type Wrapper interface {
	HasCommand(string) bool
	Preprocess()
}

var (
	rootCmd = &cobra.Command{
		Use:   "cwrapper",
		Short: "Wrapper of CraneSched commands",
		Long: `Wrapper of CraneSched commands.
This is a highly EXPERIMENTAL feature. 
If any error occurs, please refer to original commands.`,
		Version: util.Version(),
	}
	registeredWrappers = []Wrapper{
		LSFWrapper{},
		SlurmWrapper{},
	}
)

func ParseCmdArgs() {
	// Slurm Commands
	rootCmd.SetVersionTemplate(util.VersionTemplate())

	rootCmd.AddGroup(slurmGroup)
	{
		rootCmd.AddCommand(sacct())
		rootCmd.AddCommand(sacctmgr())
		rootCmd.AddCommand(salloc())
		rootCmd.AddCommand(sbatch())
		rootCmd.AddCommand(scancel())
		rootCmd.AddCommand(scontrol())
		rootCmd.AddCommand(sinfo())
		rootCmd.AddCommand(squeue())
		rootCmd.AddCommand(srun())
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

	if len(os.Args) > 2 {
		for _, wrapper := range registeredWrappers {
			if wrapper.HasCommand(os.Args[1]) {
				wrapper.Preprocess()
				break
			}
		}
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
