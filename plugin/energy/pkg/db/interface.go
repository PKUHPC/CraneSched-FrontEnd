package db

import (
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveJobEnergy(*types.JobData) error
	Close() error
}
