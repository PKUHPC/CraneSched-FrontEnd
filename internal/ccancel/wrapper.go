package ccancel

import (
	"os"
	"strings"
)

func ParseSlurm() error {
	for i := 1; i < len(os.Args); i++ {
		l := strings.Split(os.Args[i], "=")
		if len(l) != 2 {
			continue
		}
		switch l[0] {
		case "--jobname":
			os.Args[i] = "--name" + l[1]
		case "--nodelist":
			os.Args[i] = "--nodes" + l[1]
		}
	}
	return nil
}
