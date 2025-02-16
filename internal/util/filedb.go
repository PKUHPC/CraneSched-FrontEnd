package util

import (
	"encoding/json"
	"github.com/gofrs/flock"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

type Data struct {
	LeaderId int `json:"leader_id"`
}

type PersistentStorage struct {
	flock *flock.Flock
	data  Data
	file  string
}

func NewPersistentStorage(file string) *PersistentStorage {
	dir := filepath.Dir(file)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Errorf("Failed to create directories: %s %v", dir, err)
		return nil
	}

	lock := flock.New(file + ".lock") // 创建文件锁
	return &PersistentStorage{
		flock: lock,
		file:  file,
	}
}

func (ps *PersistentStorage) LoadData() error {
	err := ps.flock.RLock()
	if err != nil {
		return err
	}
	defer ps.flock.Unlock()

	file, err := os.Open(ps.file)
	if err != nil {
		if os.IsNotExist(err) {
			ps.data = Data{0}

			file, err = os.Create(ps.file)
			if err != nil {
				return err
			}
			defer file.Close()
			// write to file
			encoder := json.NewEncoder(file)
			err = encoder.Encode(&ps.data)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&ps.data)
	if err != nil {
		return err
	}

	return nil
}

func (ps *PersistentStorage) SaveData() error {
	err := ps.flock.Lock()
	if err != nil {
		return err
	}
	defer ps.flock.Unlock()

	file, err := os.Create(ps.file)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(ps.data)
	if err != nil {
		return err
	}

	return nil
}
