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
	log "github.com/sirupsen/logrus"
	"os"
)

var listWrappers = []Wrapper{&SlurmWrapper{}}

type Wrapper interface {
	Parse() error
	Name() string
}

func ParseWithWrapper() (useWrapper bool, err error) {
	if len(os.Args) < 2 {
		return false, nil
	}
	var parseFunc func() error
	for _, v := range listWrappers {
		if "--"+v.Name() == os.Args[1] {
			useWrapper = true
			log.Warn("converting from " + v.Name() + " args...")
			parseFunc = v.Parse
		}
	}
	if len(os.Args) == 2 {
		os.Args = os.Args[:1]
	} else {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	if parseFunc != nil {
		err = parseFunc()
		args := ""
		for _, v := range os.Args {
			args += " " + v
		}
		log.Info("new args:" + args)
		return true, err
	}
	return false, nil
}
