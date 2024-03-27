package wrapper

import (
	"CraneFrontEnd/internal/ccontrol"
	"os"
)

type SlurmWrapper struct {
}

func (s *SlurmWrapper) Parse() error {
	switch os.Args[0] {
	case "ccontrol":
		return ccontrol.ParseWrapped()
	}
	return nil
}

func (s *SlurmWrapper) Name() string {
	return "slurm"
}
