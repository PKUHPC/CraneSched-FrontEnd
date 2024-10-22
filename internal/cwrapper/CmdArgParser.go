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
