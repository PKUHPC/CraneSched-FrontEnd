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

package cinfo

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"strconv"
	"time"
)

func Query() error {
	reply, err := QueryClusterInfo()
	if err != nil {
		return err
	}
	if FlagJson {
		return JsonOutput(reply)
	}
	return QueryTableOutput(reply)
}

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return util.GetCraneError(util.ErrorCmdArg, err.Error())
	}

	return loopedSubQuery(interval)
}

func loopedSubQuery(interval time.Duration) error {
	for {
		fmt.Println(time.Now().String()[0:19])
		err := Query()
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
