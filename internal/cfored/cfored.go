/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package cfored

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/natefinch/lumberjack.v2"
)

type GlobalVariables struct {
	hostName string

	ctldConnected atomic.Bool

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	// Prevent the situation that de-multiplexer the two maps when
	// Cfored <--> Ctld state machine just removes the channel of a calloc
	// from ctldReplyChannelMapByPid and hasn't added the channel
	// to ctldReplyChannelMapByStep. In such a situation, Cfored <--> Ctld
	// state machine may think the channel of a calloc is not in both maps
	// but the channel is actually being moving from one map to another map.
	ctldReplyChannelMapMtx sync.Mutex

	// Used by Cfored <--> Ctld state machine to de-multiplex messages
	// Used for calloc/crun with task id not allocated.
	// A calloc is identified by its pid.
	ctldReplyChannelMapByPid map[int32]chan *protos.StreamCtldReply

	// Used by Cfored <--> Ctld state machine to de-multiplex messages from CraneCtld.
	// Cfored <--> Ctld state machine GUARANTEES that NO `nil` will be sent into these channels.
	// Used for calloc/crun with task id allocated.
	ctldReplyChannelMapByStep map[StepIdentifier]chan *protos.StreamCtldReply

	// Used by Calloc/Crun <--> Cfored state machine to multiplex messages
	// these messages will be sent to CraneCtld
	cforedRequestCtldChannel chan *protos.StreamCforedRequest

	pidStepMapMtx sync.RWMutex

	pidStepMap map[int32]StepIdentifier
}

var gVars GlobalVariables

func StartCfored(cmd *cobra.Command) {
	config := util.ParseConfig(FlagConfigFilePath)

	if config.Cfored.PidFilePath != "" {
		pidDir := filepath.Dir(config.Cfored.PidFilePath)
		if err := os.MkdirAll(pidDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create pid directory %s: %v\n", pidDir, err)
			os.Exit(1)
		}

		pidContent := strconv.Itoa(os.Getpid()) + "\n"
		if err := os.WriteFile(config.Cfored.PidFilePath, []byte(pidContent), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write pid file %s: %v\n", config.Cfored.PidFilePath, err)
			os.Exit(1)
		}

		defer func(pidFile string) {
			if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
				log.Warnf("Failed to remove pid file %s: %v", pidFile, err)
			}
		}(config.Cfored.PidFilePath)
	}

	if err := os.MkdirAll(config.CforedLogDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create log directory: %v\n", err)
		os.Exit(1)
	}

	logFile := &lumberjack.Logger{
		Filename:   filepath.Join(config.CforedLogDir, "cfored.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
	}
	log.SetOutput(io.MultiWriter(os.Stderr, logFile))

	debugLevel := FlagDebugLevel
	if !cmd.Flags().Changed("debug-level") && config.Cfored.DebugLevel != "" {
		debugLevel = config.Cfored.DebugLevel
	}
	util.InitLogger(debugLevel)

	util.DetectNetworkProxy()

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	defer gVars.globalCtxCancel()

	SetupAndRunSignalHandlerRoutine()

	gVars.ctldConnected.Store(false)

	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)

	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByStep = make(map[StepIdentifier]chan *protos.StreamCtldReply)
	gVars.pidStepMap = make(map[int32]StepIdentifier)

	gSupervisorChanKeeper = NewCranedChannelKeeper()

	hostName, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	gVars.hostName = hostName

	var wgAllRoutines sync.WaitGroup

	ctldClient := &GrpcCtldClient{
		config:           config,
		ctldReplyChannel: make(chan *protos.StreamCtldReply, 8),
	}
	wgAllRoutines.Add(1)
	go ctldClient.StartCtldClientStream(&wgAllRoutines)

	startGrpcServer(config, &wgAllRoutines)

	log.Debug("Waiting all go routines to exit...")
	wgAllRoutines.Wait()
}

func SetupAndRunSignalHandlerRoutine() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for {
			select {
			case sig := <-sigChan:
				if sig == syscall.SIGHUP {
					log.Info("Received SIGHUP signal, reloading configuration...")
					config := util.ParseConfig(FlagConfigFilePath)

					if config.Cfored.DebugLevel != "" {
						util.InitLogger(config.Cfored.DebugLevel)
						log.Infof("Log level reloaded to %s", config.Cfored.DebugLevel)
					}
				}
			case <-gVars.globalCtx.Done():
				return
			}
		}
	}()
}
