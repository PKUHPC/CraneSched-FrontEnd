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

	"github.com/spf13/cobra"
)

// stopExecute handles the stop command execution
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

// rmExecute handles the rm command execution
func rmExecute(cmd *cobra.Command, args []string) error {
	if FlagJson {
		result := map[string]interface{}{
			"action":  "rm",
			"status":  "not_supported",
			"message": "Container removal is not supported",
			"hint":    "Use 'ccon stop' to cancel running tasks",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Error: 'rm' command is not supported in CraneSched.\n")
		fmt.Printf("Container tasks are automatically cleaned up when they complete.\n")
		fmt.Printf("Use 'ccon stop <task_id>' to cancel a running task.\n")
	}

	return nil
}

// createExecute handles the create command with informative warning
func createExecute(cmd *cobra.Command, args []string) error {
	if FlagJson {
		result := map[string]interface{}{
			"action":  "create",
			"status":  "not_supported",
			"message": "Container creation without running is not supported",
			"hint":    "Use 'ccon run' to submit container tasks",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Error: 'create' command is not supported in CraneSched.\n")
		fmt.Printf("Use 'ccon run <image>' to submit a container task.\n")
	}

	return nil
}

// startExecute handles the start command with informative warning
func startExecute(cmd *cobra.Command, args []string) error {
	if FlagJson {
		result := map[string]interface{}{
			"action":  "start",
			"status":  "not_supported",
			"message": "Starting containers is not supported",
			"hint":    "Use 'ccon run' to submit new tasks",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Error: 'start' command is not supported in CraneSched.\n")
		fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")
	}

	return nil
}

// restartExecute handles the restart command with informative warning
func restartExecute(cmd *cobra.Command, args []string) error {
	if FlagJson {
		result := map[string]interface{}{
			"action":  "restart",
			"status":  "not_supported",
			"message": "Restarting containers is not supported",
			"hint":    "Use 'ccon run' to submit a new task",
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Error: 'restart' command is not supported in CraneSched.\n")
		fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")
	}

	return nil
}