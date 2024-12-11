package db

import (
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveTaskEnergy(*types.TaskData) error
	Close() error
}
