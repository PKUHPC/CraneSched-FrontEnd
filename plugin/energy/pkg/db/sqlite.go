package db

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type Database struct {
	db        *gorm.DB
	buffer    []*types.NodeData
	bufferMu  sync.Mutex
	batchSize int
}

type NodeEnergyRecord struct {
	ID        uint      `gorm:"primarykey"`
	NodeID    string    `gorm:"index"`
	ClusterID string    `gorm:"index"`
	Timestamp time.Time `gorm:"index"`

	Package     float64 // 包能耗
	Core        float64
	Uncore      float64 // 非核心能耗
	DRAM        float64
	GT          float64 // 集成显卡能耗
	Temperature float64 // 多核平均CPU温度(℃)
	Frequencies float64 // 多核平均CPU频率(MHz)

	CPUUtil    float64 // CPU使用率(%)
	MemoryUtil float64 // 内存使用率(%)
	DiskIO     float64 // 磁盘IO(MB/s)
	NetworkIO  float64 // 网络IO(MB/s)
}

type TaskEnergyRecord struct {
	ID        uint   `gorm:"primarykey"`
	TaskName  string `gorm:"index"`
	NodeID    string `gorm:"index"`
	ClusterID string `gorm:"index"`

	StartTime time.Time `gorm:"index"`
	EndTime   time.Time
	Duration  float64 // 秒

	TotalEnergy  float64
	CPUEnergy    float64
	GPUEnergy    float64
	DRAMEnergy   float64
	AveragePower float64

	CPUTime        float64
	CPUUtilization float64
	GPUUtilization float64
	MemoryUsage    float64
	MemoryUtil     float64
	DiskReadBytes  float64
	DiskWriteBytes float64

	ResourceHistory string
}

func NewSQLiteDB(config *config.DBConfig) (DBInterface, error) {
	log.Printf("\033[34m[SQLite]\033[0m Initializing SQLite with config: %+v", config)
	dialector := sqlite.Open(config.SQLite.Path)

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, err
	}

	// 自动迁移表结构
	if err := db.AutoMigrate(&NodeEnergyRecord{}, &TaskEnergyRecord{}); err != nil {
		return nil, err
	}

	database := &Database{
		db:        db,
		batchSize: config.BatchSize,
		buffer:    make([]*types.NodeData, 0, config.BatchSize),
	}

	flushTime, err := time.ParseDuration(config.FlushTime)
	if err != nil {
		log.Printf("\033[31m[SQLite]\033[0m Invalid flush time format: %v, using default 30s", err)
		flushTime = 30 * time.Second
	}

	go database.periodicFlush(flushTime)
	return database, nil
}

func (d *Database) SaveNodeEnergy(data *types.NodeData) error {
	var bufferToWrite []*types.NodeData

	d.bufferMu.Lock()
	d.buffer = append(d.buffer, data)
	if len(d.buffer) >= d.batchSize {
		bufferToWrite = d.buffer
		d.buffer = make([]*types.NodeData, 0, d.batchSize)
	}
	d.bufferMu.Unlock()

	if bufferToWrite != nil {
		return d.writeBatch(bufferToWrite)
	}
	return nil
}

func (d *Database) writeBatch(dataList []*types.NodeData) error {
	if len(dataList) == 0 {
		return nil
	}

	log.Printf("\033[34m[SQLite]\033[0m Batch saving node energy data, count: %d", len(dataList))

	records := make([]NodeEnergyRecord, 0, len(dataList))
	for _, data := range dataList {
		record := NodeEnergyRecord{
			NodeID:      data.NodeID,
			ClusterID:   data.ClusterID,
			Timestamp:   data.Timestamp,
			Package:     data.Package,
			Core:        data.Core,
			Uncore:      data.Uncore,
			DRAM:        data.DRAM,
			GT:          data.GT,
			Temperature: data.Temperature,
			Frequencies: data.Frequencies,
			CPUUtil:     data.CPUUtil,
			MemoryUtil:  data.MemoryUtil,
			DiskIO:      data.DiskIO,
			NetworkIO:   data.NetworkIO,
		}
		records = append(records, record)
	}

	// 使用事务批量插入
	return d.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&records).Error
	})
}

func (d *Database) periodicFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		var bufferToWrite []*types.NodeData

		d.bufferMu.Lock()
		if len(d.buffer) > 0 {
			bufferToWrite = d.buffer
			d.buffer = make([]*types.NodeData, 0, d.batchSize)
		}
		d.bufferMu.Unlock()

		if bufferToWrite != nil {
			if err := d.writeBatch(bufferToWrite); err != nil {
				log.Printf("\033[31m[SQLite]\033[0m Failed to flush buffer: %v", err)
			}
		}
	}
}

func (d *Database) SaveTaskEnergy(monitor *types.TaskData) error {
	history, _ := json.Marshal(monitor.TaskResourceUsage)

	record := &TaskEnergyRecord{
		TaskName:  monitor.TaskName,
		NodeID:    monitor.NodeID,
		ClusterID: monitor.ClusterID,

		StartTime: monitor.StartTime,
		EndTime:   time.Now(),
		Duration:  time.Since(monitor.StartTime).Seconds(),

		TotalEnergy:  monitor.TotalEnergy,
		CPUEnergy:    monitor.CPUEnergy,
		GPUEnergy:    monitor.GPUEnergy,
		DRAMEnergy:   monitor.DRAMEnergy,
		AveragePower: monitor.AveragePower,

		CPUTime:        monitor.CPUTime,
		CPUUtilization: monitor.CPUUtilization,
		GPUUtilization: monitor.GPUUtilization,
		MemoryUsage:    float64(monitor.MemoryUsage),
		MemoryUtil:     monitor.MemoryUtil,
		DiskReadBytes:  float64(monitor.DiskReadBytes),
		DiskWriteBytes: float64(monitor.DiskWriteBytes),

		ResourceHistory: string(history),
	}

	return d.db.Create(record).Error
}

func (d *Database) Close() error {
	// 在关闭前刷新剩余数据
	d.bufferMu.Lock()
	if len(d.buffer) > 0 {
		if err := d.writeBatch(d.buffer); err != nil {
			log.Printf("\033[31m[SQLite]\033[0m Failed to write remaining data: %v", err)
		}
	}
	d.bufferMu.Unlock()

	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
