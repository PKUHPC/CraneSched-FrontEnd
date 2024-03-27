package wrapper

import (
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
	for _, v := range listWrappers {
		if "--"+v.Name() == os.Args[1] {
			useWrapper = true
			return true, v.Parse()
		}
	}
	return false, nil
}
