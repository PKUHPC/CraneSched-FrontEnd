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
	"strings"

	"github.com/spf13/cobra"
)

type Wrapper interface {
	Group() *cobra.Group
	SubCommands() []*cobra.Command
	HasCommand(string) bool
	Preprocess() error
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
	wrappers = []Wrapper{
		LSFWrapper{},
		SlurmWrapper{},
	}
)

func ParseCmdArgs() {
	rootCmd.SetVersionTemplate(util.VersionTemplate())

	for _, wrapper := range wrappers {
		rootCmd.AddGroup(wrapper.Group())
		rootCmd.AddCommand(wrapper.SubCommands()...)
		if len(os.Args) > 2 && !strings.HasPrefix(os.Args[1], "-") && wrapper.HasCommand(os.Args[1]) {
			wrapper.Preprocess()
		}
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
