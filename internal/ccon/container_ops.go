/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccon

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func runExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("run requires at least one argument: IMAGE [COMMAND] [ARG...]")
	}

	image := args[0]
	var command []string
	if len(args) > 1 {
		command = args[1:]
	}
	containerName := FlagName
	if containerName == "" {
		containerName = fmt.Sprintf("container_%d", time.Now().Unix())
	}

	if FlagJson {
		result := map[string]interface{}{
			"action":     "run",
			"container":  containerName,
			"image":      image,
			"status":     "success",
			"detached":   FlagDetach,
			"entrypoint": FlagEntrypoint,
			"command":    command,
			"user":       FlagUser,
			"userns":     FlagUserNS,
			"workdir":    FlagWorkdir,
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Container created and started: %s (from image %s)\n", containerName, image)
		if FlagDetach {
			fmt.Printf("  Running in background (detached mode)\n")
		} else {
			fmt.Printf("  Running in foreground\n")
		}
		if FlagEntrypoint != "" {
			fmt.Printf("  Entrypoint: %s\n", FlagEntrypoint)
		}
		if len(command) > 0 {
			fmt.Printf("  Command: %s\n", strings.Join(command, " "))
		}
		if len(FlagPorts) > 0 {
			fmt.Printf("  Port mappings: %s\n", strings.Join(FlagPorts, ", "))
		}
		if len(FlagEnv) > 0 {
			fmt.Printf("  Environment variables: %s\n", strings.Join(FlagEnv, ", "))
		}
		if len(FlagVolume) > 0 {
			fmt.Printf("  Volume mounts: %s\n", strings.Join(FlagVolume, ", "))
		}
		if FlagWorkdir != "" {
			fmt.Printf("  Working directory: %s\n", FlagWorkdir)
		}
		if FlagUserNS {
			if FlagUser != "" {
				fmt.Printf("  User namespace: enabled (user: %s as fake root)\n", FlagUser)
			} else {
				fmt.Printf("  User namespace: enabled (default user: fake root)\n")
			}
		} else {
			if FlagUser != "" {
				fmt.Printf("  User namespace: disabled (user: %s, limited to current user and accessible groups)\n", FlagUser)
			} else {
				fmt.Printf("  User namespace: disabled\n")
			}
		}
	}

	return nil
}

func stopExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("stop requires exactly one argument: CONTAINER")
	}

	container := args[0]

	if FlagJson {
		result := map[string]interface{}{
			"action":    "stop",
			"container": container,
			"timeout":   FlagTimeout,
			"status":    "success",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Container stopped: %s (timeout: %d seconds)\n", container, FlagTimeout)
	}

	return nil
}

func rmExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("rm requires exactly one argument: CONTAINER")
	}

	container := args[0]

	if FlagJson {
		result := map[string]interface{}{
			"action":    "remove",
			"container": container,
			"force":     FlagForce,
			"status":    "success",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		if FlagForce {
			fmt.Printf("Container forcefully removed: %s\n", container)
		} else {
			fmt.Printf("Container removed: %s\n", container)
		}
	}

	return nil
}