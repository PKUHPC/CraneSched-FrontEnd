package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
)

var (
	ps     *PersistentStorage
	psOnce sync.Once
)

func GetLeaderIdFromFile() int {
	psOnce.Do(func() {
		ps = NewPersistentStorage(DefaultTempBaseDir + DefaultPersistentDataPath)
	})
	if ps == nil {
		return -2
	}
	err := ps.LoadData()
	if err != nil {
		return -2
	}
	return ps.data.LeaderId
}

func UpdateLeaderIdToFile(leaderId int) error {
	if ps == nil {
		return fmt.Errorf("persistent storage not initialized")
	}
	ps.data.LeaderId = leaderId
	err := ps.SaveData()
	if err != nil {
		log.Errorf("Failed to save leader ID to file: %v", err)
		return err
	}
	return nil
}

func CurrentLeaderId() int {
	if ps == nil {
		return -2
	}
	return ps.data.LeaderId
}

func HostName2ServerId(config *Config, hostname string) int {
	for i := 0; i < len(config.CraneCtldConfig.ControlMachines); i++ {
		if config.CraneCtldConfig.ControlMachines[i].Hostname == hostname {
			return i
		}
	}

	return -2
}

func QueryLeaderFromCtld(config *Config) int {
	fmt.Println("Attempting to query current leader ID")
	if ps == nil {
		log.Error("Persistent storage not initialized")
		return -2
	}
	var stub protos.CraneCtldClient
	l := len(config.CraneCtldConfig.ControlMachines)
	// Skip the first control machine (index 0) to avoid querying self
	for i := 1; i < l; i++ {
		id := (ps.data.LeaderId + i) % l
		stub = GetStubToCtldByConfigAndLeaderId(config, id)

		req := &protos.QueryLeaderIdRequest{}

		reply, err := stub.QueryLeaderId(context.Background(), req)
		if err == nil {
			if reply.LeaderId >= 0 {
				return int(reply.LeaderId)
			} else {
				fmt.Printf("server #%d return to an abnormal state, leader ID: %d\n", id, reply.LeaderId)
			}
		}
	}
	return -2
}

func QueryAndUpdateLeaderId(config *Config) {
	id := QueryLeaderFromCtld(config)
	if id >= 0 {
		UpdateLeaderIdToFile(id)
		log.Printf("Leader ID was changed to %d, please try again!\n", id)
	} else {
		log.Errorln("Failed to query current leader ID, broken backend.")
	}
}
