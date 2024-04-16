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

package cacctmgr

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

func ParseSlurm() error {
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "create":
			os.Args[i] = "add"
		case "remove":
			os.Args[i] = "delete"
		case "show":
			os.Args[i] = "find"
		case "list":
			os.Args[i] = "find"
		case "set":
			if i+1 < len(os.Args) {
				os.Args = append(os.Args[:i], os.Args[i+1:]...)
				os.Args[i] = "--" + strings.ToLower(os.Args[i])
			}
		default:
			l := strings.Split(os.Args[i], "=")
			if len(l) != 2 {
				continue
			}
			switch strings.ToLower(l[0]) {
			case "description":
				os.Args[i] = "--description=" + l[1]
			case "name":
				os.Args[i] = "--name=" + l[1]
			case "account":
				os.Args[i] = "--account=" + l[1]
			}
		}
	}
	if len(os.Args) >= 4 {
		if os.Args[1] == "add" || os.Args[1] == "modify" || os.Args[1] == "update" {
			if strings.Contains(os.Args[3], ",") {
				log.Fatal("cacctmgr-add-account allows one account only")
			} else {
				os.Args[3] = "-N=" + os.Args[3]
			}
		} else if os.Args[1] == "find" {
			if strings.Contains(os.Args[3], "=") && len(os.Args) == 4 {
				os.Args = os.Args[:4]
			} else if os.Args[3] == "where" && len(os.Args) > 4 {
				os.Args = append(os.Args[:3], os.Args[4])
				if strings.Contains(os.Args[3], "=") {
					l := strings.Split(os.Args[3], "=")
					os.Args[3] = l[1]
				}
			}
			if len(os.Args) == 3 {
				os.Args[1] = "show"
				os.Args[2] += "s"
			}
		}
	}
	return nil
}
