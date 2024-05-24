package cbatch

import (
	"errors"
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
			log.Warnf("warning: slurm arg %v is not supported", split[1])
		}
	} else if len(split) == 2 {
		parts := strings.Split(split[1], "=")
		ok := s.supported[parts[0]]
		if ok && len(parts) > 1 {
			*args = append(*args, CbatchArg{name: parts[0], val: parts[1]})
		} else {
			log.Warnf("warning: slurm arg %v is not supported", parts[0])
		}
	} else {
		return errors.New("fields out of bound")
	}
	return nil
}

// for sh commands
type defaultProcessor struct {
}

func (d *defaultProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	*sh = append(*sh, line)
	return nil
}
