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

package cbatch

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type LineProcessor interface {
	Process(line string, sh *[]string, args *[]CbatchArg) error
}

// For Crane args
type cLineProcessor struct {
}

func (c *cLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	split := strings.Fields(line)
	if len(split) == 3 {
		*args = append(*args, CbatchArg{name: split[1], val: split[2]})
	} else if len(split) == 2 {
		*args = append(*args, CbatchArg{name: split[1]})
	} else {
		return errors.New("fields out of bound")
	}
	return nil
}

// For Slurm args
type sLineProcessor struct {
	supported map[string]bool
}

func (s *sLineProcessor) init() {
	s.supported = map[string]bool{
		"-c": true, "--cpus-per-task": true, "-J": true, "--job-name": true, "-N": true, "--qos": true, "Q": true,
		"--nodes": true, "-A": true, "--account": true, "-e": true, "--exclude": true, "--chdir": true,
		"--export": true, "--mem": true, "-p": true, "--partition": true, "-o": true, "--output": true,
		"--nodelist": true, "-w": true, "--get-user-env": true, "--time": true, "-t": true, "--ntasks-per-node": true,
	}
}

func (s *sLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	if s.supported == nil {
		s.init()
	}
	split := strings.Fields(line)
	if len(split) == 3 {
		ok := s.supported[split[1]]
		if ok {
			*args = append(*args, CbatchArg{name: split[1], val: split[2]})
		} else {
			log.Warnf("Slurm option %v is not supported", split[1])
		}
	} else if len(split) == 2 {
		parts := strings.Split(split[1], "=")
		ok := s.supported[parts[0]]
		if ok {
			if len(parts) > 1 {
				*args = append(*args, CbatchArg{name: parts[0], val: parts[1]})
			} else {
				*args = append(*args, CbatchArg{name: parts[0]})
			}
		} else {
			return fmt.Errorf("line `%v` is not supported by cwrapper", line)
		}
	} else {
		return errors.New("fields out of bound")
	}
	return nil
}

// for LSF args
type lLineProcessor struct {
	mapping map[string]string
}

func (l *lLineProcessor) init() {
	l.mapping = map[string]string{
		"-J": "-J", "-o": "-o", "-e": "-e", "-nnode": "--nodes",
		"-n": "--ntasks-per-node", "-W": "--time", "-M": "--mem", "-cwd": "--chdir",
		"-q": "--partition", "-env": "--export",
	}
}

func (l *lLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	if l.mapping == nil {
		l.init()
	}
	split := strings.Fields(line)
	if len(split) == 3 {
		if name, ok := l.mapping[split[1]]; ok {
			val := split[2]
			if name == "--time" {
				val = ConvertLSFRuntimeLimit(val)
			}
			*args = append(*args, CbatchArg{name: name, val: val})
		} else {
			log.Warnf("LSF option %v is not supported", split[1])
		}
	} else {
		return fmt.Errorf("line `%v` is not supported by cwrapper", line)
	}
	return nil
}

func ConvertLSFRuntimeLimit(t string) string {
	if t == "" {
		return t
	}
	// [hour:]minute
	re := regexp.MustCompile(`(?:(\d+):)?(\d+)`)
	x := re.FindStringSubmatch(t)
	if x[0] != t {
		log.Fatalf("Failed to parse LSF time format: %s\n", t)
	}
	H, M := x[1], x[2]
	if H == "" {
		H = "0"
	}
	return fmt.Sprintf("%s:%s:0", H, M)
}

// for sh commands
type defaultProcessor struct {
}

func (d *defaultProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	*sh = append(*sh, line)
	return nil
}
