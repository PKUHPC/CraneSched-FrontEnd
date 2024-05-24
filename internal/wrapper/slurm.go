/**
 * Copyright (c) 2023 Peking University and Peking University
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

package wrapper

import (
	"CraneFrontEnd/internal/cacctmgr"
	"CraneFrontEnd/internal/ccancel"
	"os"
	"strings"
)

type SlurmWrapper struct {
}

func (s *SlurmWrapper) Parse() error {
	l := strings.Split(os.Args[0], "/")
	os.Args[0] = l[len(l)-1]
	switch os.Args[0] {
	case "cacctmgr":
		return cacctmgr.ParseSlurm()
	case "ccancel":
		return ccancel.ParseSlurm()
	}
	return nil
}

func (s *SlurmWrapper) Name() string {
	return "slurm"
}
