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

package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func RemoveFileIfExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		err := os.Remove(path)
		if err != nil {
			log.Fatalf("Failed to remove file %s: %s", path, err.Error())
			return false
		}
	}
	return true
}

func DetectNetworkProxy() {
	envHttpProxy, ok := os.LookupEnv("http_proxy")
	if ok && envHttpProxy != "" {
		log.Warningf("http_proxy is set: %s", envHttpProxy)
	}

	envHttpsProxy, ok := os.LookupEnv("https_proxy")
	if ok && envHttpsProxy != "" {
		log.Warningf("https_proxy is set: %s", envHttpsProxy)
	}
}

func GetPidFromPort(port uint16) (int, error) {
	// 1. Find inode number for the port
	portHex := fmt.Sprintf("%04X", port)
	tcpFilePath := "/proc/net/tcp"
	tcpFileContent, err := os.ReadFile(tcpFilePath)
	if err != nil {
		return -1, err
	}
	tcpLines := strings.Split(string(tcpFileContent), "\n")[1:] // Skip header line
	var inode uint64
	inodeFound := false
	for _, line := range tcpLines {
		fields := strings.Fields(line)
		if len(fields) >= 10 {
			localAddr := fields[1]
			if strings.HasSuffix(localAddr, ":"+portHex) {
				inode, _ = strconv.ParseUint(fields[9], 10, 64)
				inodeFound = true
				break
			}
		}
	}
	if !inodeFound {
		return -1, fmt.Errorf("no inode found for port %d", port)
	}

	// 2. Find PID that is using the inode
	pid := -1
	procPath := "/proc"
	procDirs, _ := os.ReadDir(procPath)
	for _, dir := range procDirs {
		if dir.IsDir() {
			pidStr := dir.Name()
			fdPath := fmt.Sprintf("%s/%s/fd", procPath, pidStr)
			_, err := os.ReadDir(fdPath)
			if err != nil {
				continue
			}
			fdLinks, _ := os.ReadDir(fdPath)
			for _, fdLink := range fdLinks {
				fdPath := fmt.Sprintf("%s/%s/fd/%s", procPath, pidStr, fdLink.Name())
				stat, err := os.Stat(fdPath)
				if err != nil {
					continue
				}
				sysStat := stat.Sys().(*syscall.Stat_t)

				if (sysStat.Mode&syscall.S_IFMT) == syscall.S_IFSOCK && sysStat.Ino == inode {
					pid, _ = strconv.Atoi(pidStr)
					break
				}

			}
		}
		if pid != -1 {
			break
		}
	}
	if pid == -1 {
		return -1, fmt.Errorf("no process found for port %d", port)
	}
	return pid, nil
}

func GetParentProcessID(pid int) (int, error) {
	// Construct the path to the procfs entry for the process
	procfsPath := fmt.Sprintf("/proc/%d/stat", pid)

	// Read the stat file for the process
	statBytes, err := os.ReadFile(procfsPath)
	if err != nil {
		return 0, err
	}

	// Split the stat file content into fields
	statFields := strings.Fields(string(statBytes))

	// Parse the parent process ID from the fields
	ppid, err := strconv.Atoi(statFields[3])
	if err != nil {
		return 0, err
	}

	return ppid, nil
}
