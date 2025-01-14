package db

import (
	"github.com/PKUHPC/CraneSched-FrontEnd/plugin/energy/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveTaskEnergy(*types.TaskData) error
	Close() error
}
