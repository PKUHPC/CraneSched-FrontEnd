/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package calloc

import (
	"CraneFrontEnd/internal/util"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pkg/term/termios"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func StartTerminal(shellPath string,
	cancelRequestChannel chan bool,
	terminalExitChannel chan bool) {

	callocPid := unix.Getpid()

	pgrp := syscall.Getpgrp()
	log.Tracef("Pgrp: %d", pgrp)

	ptyAttr := unix.Termios{}
	err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
	if err != nil {
		log.Errorf("tcgetattr: %v", err)
		os.Exit(util.ErrorGeneric)
	}

	log.Tracef("IsForeGround: %v", util.IsForeground())

	err = unix.Setpgid(callocPid, callocPid)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorGeneric)
	}

	err = util.TcSetpgrp(0, callocPid)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorGeneric)
	}

	process := exec.Command(shellPath, "-i")
	process.Stdin = os.Stdin
	process.Stdout = os.Stdout
	process.Stderr = os.Stderr
	process.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
		Pgid:    0,

		Ctty:       0,
		Foreground: true,
	}
	process.Env = os.Environ()

	err = process.Start()
	if err != nil {
		log.Errorf("Failed to call process.Start(): %v", err)
		os.Exit(util.ErrorGeneric)
	}

	log.Tracef("Proc.Pid: %d", process.Process.Pid)

	processPgid, err := unix.Getpgid(process.Process.Pid)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorGeneric)
	}
	log.Tracef("Proc.Pgid: %d", processPgid)

	sigs := make(chan os.Signal, 1)
	sigsListenerDone := make(chan bool, 1)
	var sigsListenerWg sync.WaitGroup

	signal.Notify(sigs, syscall.SIGHUP)
	signal.Ignore(syscall.SIGTSTP, syscall.SIGTTIN, syscall.SIGTTOU)

	sigsListenerWg.Add(1)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()

	loop:
		for {
			select {

			case sig := <-sigs:
				log.Tracef("Signal received: %v", sig)
				switch sig {
				case syscall.SIGHUP:
					signalErr := process.Process.Signal(sig)
					if signalErr != nil {
						log.Trace(signalErr)
					}
				default:
					log.Tracef("Ignored signal: %v", sig)
				}

			case <-sigsListenerDone:
				break loop
			}
		}
		log.Tracef("Signal processing goroutine exit.")
	}(&sigsListenerWg)

	err = util.TcSetpgrp(0, process.Process.Pid)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorGeneric)
	}

	// Listen to cancel request
	cancelListenerDone := make(chan bool, 1)
	var cancelListenerWg sync.WaitGroup

	cancelListenerWg.Add(1)

	go func(procPid int, cancelRequestChannel chan bool,
		wg *sync.WaitGroup, done chan bool) {
	cancelListenLoop:
		for {
			select {
			case <-cancelRequestChannel:
				log.Tracef("Killing terminal with SIGHUP")
				err := syscall.Kill(procPid, syscall.SIGHUP)
				if err != nil {
					log.Errorln(err)
					os.Exit(util.ErrorGeneric)
				}

			case <-done:
				break cancelListenLoop
			}
		}
		wg.Done()
	}(process.Process.Pid, cancelRequestChannel,
		&cancelListenerWg, cancelListenerDone)

	procWaitErr := process.Wait()

	// Restore calloc terminal

	err = util.TcSetpgrp(0, callocPid)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorGeneric)
	}

	if procWaitErr != nil {
		log.Tracef("Failed to call process.Run(): %v", procWaitErr)
		exitError, ok := procWaitErr.(*exec.ExitError)
		if !ok {
			log.Tracef("Failed to convert err returned by process.Run() to os.ExitError")
		}

		if exitError.Success() {
			log.Tracef("Proc exited with code %d", exitError.ExitCode())
		} else {
			waitStatus := exitError.Sys().(syscall.WaitStatus)
			log.Tracef("Proc was killed by signal: %t. "+
				"Proc was stopped(SIGSTP): %t. Signal received: %s",
				waitStatus.Signaled(), waitStatus.Stopped(), waitStatus.Signal().String())
		}
	} else {
		log.Tracef("Proc exited with code: 0")
	}

	sigsListenerDone <- true
	cancelListenerDone <- true

	sigsListenerWg.Wait()
	cancelListenerWg.Wait()

	err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
	if err != nil {
		log.Errorf("tcsetattr: %v", err)
		os.Exit(util.ErrorGeneric)
	}

	terminalExitChannel <- true
}
