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

package cqueue

import (
	"os"
	"strings"
)

func ParseSlurm() error {
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-h":
			os.Args[i] = "-N"
		case "-S":
			if i+2 < len(os.Args) {
				os.Args = append(os.Args[:i], os.Args[i+2])
			} else {
				os.Args = os.Args[:i]
			}
		default:
			l := strings.Split(os.Args[i], "=")
			if len(l) != 2 {
				continue
			}
			switch l[0] {
			case "--account":
				os.Args[i] = "--Account" + l[1]
			}
		}
	}
	return nil
}
