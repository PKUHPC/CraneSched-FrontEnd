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
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// buildContainerTask creates a TaskToCtld with container metadata from command line arguments
func buildContainerTask(image string, command []string) (*protos.TaskToCtld, error) {
	task := &protos.TaskToCtld{
		Type:      protos.TaskType_Container,
		TimeLimit: util.InvalidDuration(),
		ReqResources: &protos.ResourceView{
			AllocatableRes: &protos.AllocatableResource{
				CpuCoreLimit:       1,
				MemoryLimitBytes:   0,
				MemorySwLimitBytes: 0,
			},
		},
		NodeNum:        1,
		NtasksPerNode:  1,
		CpusPerTask:    1,
		GetUserEnv:     false,
		Env:            make(map[string]string),
	}

	// Parse image reference to extract registry, repository, and tag
	registry, repository, tag := parseImageRef(image)
	fullImageName := repository + ":" + tag
	if registry != "" {
		fullImageName = registry + "/" + fullImageName
	}

	// Get authentication info for the registry
	username, password, err := getAuthForRegistry(registry)
	if err != nil {
		log.Warnf("Failed to get auth for registry %s: %v", registry, err)
		username, password = "", ""
	}

	// Create container metadata
	containerMeta := &protos.ContainerTaskAdditionalMeta{
		Image: &protos.ContainerTaskAdditionalMeta_ImageInfo{
			Image:         fullImageName,
			Username:      username,
			Password:      password,
			ServerAddress: registry,
		},
		Userns:   FlagUserNS,
		Detached: FlagDetach,
		Env:      make(map[string]string),
		Mounts:   make(map[string]string),
		Ports:    make(map[int32]int32),
	}

	// Set container name
	if FlagName != "" {
		containerMeta.Name = FlagName
		task.Name = FlagName
	} else {
		containerName := fmt.Sprintf("container_%d", time.Now().Unix())
		containerMeta.Name = containerName
		task.Name = containerName
	}

	// Set entrypoint and command
	if FlagEntrypoint != "" {
		containerMeta.Entrypoint = FlagEntrypoint
	}
	if len(command) > 0 {
		containerMeta.Command = command[0]
		if len(command) > 1 {
			containerMeta.Args = command[1:]
		}
	}

	// Set working directory
	if FlagWorkdir != "" {
		containerMeta.Workdir = FlagWorkdir
	}

	// Parse and set user information
	if FlagUser != "" {
		if err := parseUserSpec(FlagUser, containerMeta); err != nil {
			return nil, fmt.Errorf("invalid user specification '%s': %v", FlagUser, err)
		}
	}

	// Parse and set environment variables
	for _, env := range FlagEnv {
		if err := parseEnvVar(env, containerMeta.Env); err != nil {
			return nil, fmt.Errorf("invalid environment variable '%s': %v", env, err)
		}
	}

	// Parse and set port mappings
	for _, port := range FlagPorts {
		if err := parsePortMapping(port, containerMeta.Ports); err != nil {
			return nil, fmt.Errorf("invalid port mapping '%s': %v", port, err)
		}
	}

	// Parse and set volume mounts
	for _, volume := range FlagVolume {
		if err := parseVolumeMount(volume, containerMeta.Mounts); err != nil {
			return nil, fmt.Errorf("invalid volume mount '%s': %v", volume, err)
		}
	}

	// Set the container metadata as payload
	task.Payload = &protos.TaskToCtld_ContainerMeta{
		ContainerMeta: containerMeta,
	}

	return task, nil
}

// parseUserSpec parses user specification in format "user" or "user:group"
func parseUserSpec(userSpec string, containerMeta *protos.ContainerTaskAdditionalMeta) error {
	parts := strings.SplitN(userSpec, ":", 2)

	// Parse user (can be name or UID)
	user := parts[0]
	if uid, err := strconv.ParseUint(user, 10, 32); err == nil {
		containerMeta.RunAsUser = uint32(uid)
	} else {
		// For user names, we would need to resolve to UID
		// For now, assume it's provided as UID
		return fmt.Errorf("user name resolution not implemented, please provide UID")
	}

	// Parse group if provided
	if len(parts) == 2 {
		group := parts[1]
		if gid, err := strconv.ParseUint(group, 10, 32); err == nil {
			containerMeta.RunAsGroup = uint32(gid)
		} else {
			return fmt.Errorf("group name resolution not implemented, please provide GID")
		}
	}

	return nil
}

// parseEnvVar parses environment variable in format "KEY=VALUE"
func parseEnvVar(envVar string, envMap map[string]string) error {
	parts := strings.SplitN(envVar, "=", 2)
	if len(parts) != 2 {
		return errors.New("environment variable must be in KEY=VALUE format")
	}
	envMap[parts[0]] = parts[1]
	return nil
}

// parsePortMapping parses port mapping in format "host:container" or "port"
func parsePortMapping(portSpec string, portMap map[int32]int32) error {
	parts := strings.SplitN(portSpec, ":", 2)

	if len(parts) == 1 {
		// Single port, map to same port
		port, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid port number: %v", err)
		}
		portMap[int32(port)] = int32(port)
	} else {
		// Host:container format
		hostPort, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid host port: %v", err)
		}
		containerPort, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid container port: %v", err)
		}
		portMap[int32(hostPort)] = int32(containerPort)
	}

	return nil
}

// parseVolumeMount parses volume mount in format "host:container" or "volume"
func parseVolumeMount(volumeSpec string, mountMap map[string]string) error {
	parts := strings.SplitN(volumeSpec, ":", 2)

	if len(parts) != 2 {
		return errors.New("volume mount must be in HOST:CONTAINER format")
	}

	hostPath := parts[0]
	containerPath := parts[1]

	// Basic validation
	if hostPath == "" || containerPath == "" {
		return errors.New("host path and container path cannot be empty")
	}

	mountMap[hostPath] = containerPath
	return nil
}

// runExecute handles the run command execution
func runExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("run requires at least one argument: IMAGE [COMMAND] [ARG...]")
	}

	image := args[0]
	var command []string
	if len(args) > 1 {
		command = args[1:]
	}

	// Build the container task
	task, err := buildContainerTask(image, command)
	if err != nil {
		return fmt.Errorf("failed to build container task: %v", err)
	}

	// For debugging: print task details if JSON flag is set
	if FlagJson {
		if taskJSON, err := json.MarshalIndent(task, "", "  "); err == nil {
			fmt.Printf("DEBUG: Task structure:\n%s\n", string(taskJSON))
		}
	}

	// Submit the task
	return submitContainerTask(task)
}

// submitContainerTask submits a container task via gRPC
func submitContainerTask(task *protos.TaskToCtld) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTaskRequest{Task: task}

	reply, err := stub.SubmitBatchTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit the container task")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Container task submitted successfully. Task ID: %d\n", reply.GetTaskId())
		if task.GetContainerMeta().Detached {
			fmt.Printf("Container '%s' is running in detached mode.\n", task.GetContainerMeta().Name)
		} else {
			fmt.Printf("Container '%s' is starting in foreground mode.\n", task.GetContainerMeta().Name)
		}
		return nil
	} else {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Container task submission failed: %s", util.ErrMsg(reply.GetCode())),
		}
	}
}