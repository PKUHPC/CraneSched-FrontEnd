package template

type Vars struct {
	GRES map[string]string
	ARGS map[string]string
}

func BuildVars(gresDevice, gresIndex string, cniArgs map[string]string) Vars {
	argsCopy := make(map[string]string, len(cniArgs))
	for k, v := range cniArgs {
		argsCopy[k] = v
	}

	return Vars{
		GRES: map[string]string{
			"device": gresDevice,
			"index":  gresIndex,
		},
		ARGS: argsCopy,
	}
}
