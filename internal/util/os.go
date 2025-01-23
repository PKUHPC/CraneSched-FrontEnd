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

package util

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func RemoveFileIfExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		err := os.Remove(path)
		if err != nil {
			log.Errorf("Failed to remove file %s: %v", path, err)
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

func GetUidByUserName(userName string) (uint32, error) {
	u, err := user.Lookup(userName)
	if err != nil {
		return 0, err
	}

	i64, err := strconv.ParseInt(u.Uid, 10, 64)
	if err != nil {
		// This should never happen
		return 0, err
	}
	uid := uint32(i64)

	return uid, nil
}

func GetX11Display() (string, uint32, error) {
	display := os.Getenv("DISPLAY")
	if display == "" {
		return "", 0, errors.New("DISPLAY environment variable not set")
	}

	displayRegex := regexp.MustCompile(`^(?P<host>[a-zA-Z0-9._-]*):(?P<display>\d+)\.(?P<screen>\d+)$`)
	match := displayRegex.FindStringSubmatch(display)
	if match == nil {
		return "", 0, fmt.Errorf("invalid DISPLAY format: %s", display)
	}

	host := match[1]
	port, err := strconv.ParseUint(match[2], 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("invalid DISPLAY format: %s", display)
	}

	log.Debugf("X11 host: %s, port: %d", host, port)

	return host, uint32(port), nil
}

func GetX11AuthCookie() (string, error) {
	const cookiePattern = `(?m)[a-zA-Z0-9./_-]+:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`
	const wildcardPattern = `(?m)#ffff#[0-9a-fA-F./_-]+#:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`

	display := os.Getenv("DISPLAY")
	if display == "" {
		return "", errors.New("DISPLAY environment variable not set")
	}

	cmd := exec.Command("xauth", "list", display)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Debugf("Error running xauth command: %v. Stderr: %s", err, stderr.String())
		return "", fmt.Errorf("failed to run xauth: %v", err)
	}

	log.Debugf("Got xauth cookies: %s", stdout.String())

	result := stdout.String()
	cookieRegex := regexp.MustCompile(cookiePattern)
	wildcardRegex := regexp.MustCompile(wildcardPattern)

	match := cookieRegex.FindStringSubmatch(result)
	if match == nil {
		log.Debugf("MAGIC cookie not found, checking wildcard cookies")
		match = wildcardRegex.FindStringSubmatch(result)
		if match == nil {
			return "", errors.New("MAGIC cookie not found")
		}
	}

	if len(match) < 2 {
		return "", errors.New("invalid format for MAGIC cookie")
	}

	return match[1], nil
}
