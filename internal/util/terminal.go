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
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"strings"
)

func NixShell(uid string) (string, error) {
	out, err := exec.Command("getent", "passwd", uid).Output()
	if err != nil {
		return "", err
	}

	ent := strings.Split(strings.TrimSuffix(string(out), "\n"), ":")
	return ent[6], nil
}

// TcGetpgrp gets the process group ID of the foreground process
// group associated with the terminal referred to by fd.
//
// See POSIX.1 documentation for more details:
// http://pubs.opengroup.org/onlinepubs/009695399/functions/tcgetpgrp.html
func TcGetpgrp(fd int) (pgrp int, err error) {
	return unix.IoctlGetInt(fd, unix.TIOCGPGRP)
}

// TcSetpgrp sets the foreground process group ID associated with the
// terminal referred to by fd to pgrp.
//
// See POSIX.1 documentation for more details:
// https://pubs.opengroup.org/onlinepubs/9699919799/functions/tcsetpgrp.html
func TcSetpgrp(fd int, pgrp int) (err error) {
	return unix.IoctlSetPointerInt(fd, unix.TIOCSPGRP, pgrp)
}

// IsForeground returns true if the calling process is a foreground process.
//
// Note that the foreground/background status of a process can change
// at any moment if the user utilizes the shell job control commands (fg/bg).
//
// Example use for command line tools: suppress extra output if a
// process is running in background, provide verbose output when
// running on foreground.
func IsForeground() bool {
	pgrp1, err := TcGetpgrp(int(os.Stdin.Fd()))
	if err != nil {
		return false
	}
	pgrp2 := unix.Getpgrp()
	return pgrp1 == pgrp2
}
