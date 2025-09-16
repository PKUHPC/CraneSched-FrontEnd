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
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

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

// parseEnvVar parses environment variable in format "KEY=VALUE" or "KEY"
func parseEnvVar(envVar string, envMap map[string]string) error {
	parts := strings.SplitN(envVar, "=", 2)
	if len(parts) == 1 {
		// KEY format - set empty value
		envMap[parts[0]] = ""
	} else if len(parts) == 2 {
		// KEY=VALUE format
		envMap[parts[0]] = parts[1]
	} else {
		return errors.New("environment variable must be in KEY=VALUE or KEY format")
	}
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

	// Validate container-specific parameters
	if err := validateContainerTask(task); err != nil {
		return fmt.Errorf("container validation failed: %v", err)
	}

	// Submit the task
	return submitContainerTask(task)
}

// applyResourceOptions applies resource options to the task
func applyResourceOptions(task *protos.TaskToCtld) error {
	// CPU allocation
	if FlagCpus > 0 {
		task.CpusPerTask = FlagCpus
	} else {
		// Default to 1 CPU if not specified
		task.CpusPerTask = 1
	}

	// Memory allocation - prefer --mem over --memory
	memorySpec := ""
	if FlagMem != "" {
		memorySpec = FlagMem
	} else if FlagMemory != "" {
		log.Warn("--memory is deprecated, please use --mem instead")
		memorySpec = FlagMemory
	}

	if memorySpec != "" {
		memoryBytes, err := util.ParseMemStringAsByte(memorySpec)
		if err != nil {
			return fmt.Errorf("invalid memory specification '%s': %v", memorySpec, err)
		}
		task.ReqResources.AllocatableRes.MemoryLimitBytes = memoryBytes
		task.ReqResources.AllocatableRes.MemorySwLimitBytes = memoryBytes
	}

	// GPU allocation - prefer --gres over --gpus
	gresSpec := ""
	if FlagGres != "" {
		gresSpec = FlagGres
	} else if FlagGpus != "" {
		return errors.New("--gpus is not supported due to format complexity. Please use --gres instead with format like 'gpu:1' or 'gpu:a100:2'")
	}

	if gresSpec != "" {
		// Set GPU resources via DeviceMap (like cbatch does)
		task.ReqResources.DeviceMap = util.ParseGres(gresSpec)
	}

	return nil
}

// applySchedulingOptions applies cluster scheduling options to the task
func applySchedulingOptions(task *protos.TaskToCtld) error {
	// Partition/queue assignment
	if FlagPartition != "" {
		task.PartitionName = FlagPartition
	}

	// Time limit
	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return fmt.Errorf("invalid time specification '%s': %v", FlagTime, err)
		}
		task.TimeLimit.Seconds = seconds
	}

	// Account
	if FlagAccount != "" {
		task.Account = FlagAccount
	}

	// QoS
	if FlagQos != "" {
		task.Qos = FlagQos
	}

	// Node allocation - validate parameters
	if FlagNodes > 0 {
		task.NodeNum = FlagNodes
	} else {
		task.NodeNum = 1
	}

	if FlagNtasksPerNode > 0 {
		task.NtasksPerNode = FlagNtasksPerNode
	} else {
		task.NtasksPerNode = 1
	}

	// Node list (specific nodes)
	if FlagNodelist != "" {
		task.Nodelist = FlagNodelist
	}

	// Exclude nodes
	if FlagExcludes != "" {
		task.Excludes = FlagExcludes
	}

	// Reservation
	if FlagReservation != "" {
		task.Reservation = FlagReservation
	}

	// Extra attributes - handle both individual flags and JSON like cbatch
	structExtraFromCli := util.JobExtraAttrs{}

	if FlagExtraAttr != "" {
		structExtraFromCli.ExtraAttr = FlagExtraAttr
	}
	if FlagMailType != "" {
		structExtraFromCli.MailType = FlagMailType
	}
	if FlagMailUser != "" {
		structExtraFromCli.MailUser = FlagMailUser
	}
	if FlagComment != "" {
		structExtraFromCli.Comment = FlagComment
	}

	// Marshal extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return fmt.Errorf("invalid extra attributes: %v", err)
	}
	task.ExtraAttr = extraFromCli

	// Exclusive node allocation
	task.Exclusive = FlagExclusive

	// Hold job submission
	task.Hold = FlagHold

	return nil
}

// applyEnvironmentOptions applies environment-related options to the task
func applyEnvironmentOptions(task *protos.TaskToCtld) error {
	// Set CWD if not already set
	if task.Cwd == "" {
		task.Cwd, _ = os.Getwd()
	}

	// Set UID/GID for task execution
	task.Uid = uint32(os.Getuid())
	task.Gid = uint32(os.Getgid())

	// Set command line for auditing
	task.CmdLine = strings.Join(os.Args, " ")

	return nil
}

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
		NodeNum:       1,
		NtasksPerNode: 1,
		CpusPerTask:   1,
		GetUserEnv:    false,
		Env:           make(map[string]string),
	}

	// Apply resource control options (Docker-style, mapped to cbatch semantics)
	if err := applyResourceOptions(task); err != nil {
		return nil, fmt.Errorf("failed to apply resource options: %v", err)
	}

	// Apply cluster scheduling options
	if err := applySchedulingOptions(task); err != nil {
		return nil, fmt.Errorf("failed to apply scheduling options: %v", err)
	}

	// Apply environment options
	if err := applyEnvironmentOptions(task); err != nil {
		return nil, fmt.Errorf("failed to apply environment options: %v", err)
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
		containerMeta.Name = ""
		task.Name = ""
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

	// Parse and set environment variables (container-specific)
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

// validateContainerTask validates container-specific parameters
func validateContainerTask(task *protos.TaskToCtld) error {
	containerMeta := task.GetContainerMeta()
	if containerMeta == nil {
		return errors.New("container metadata is missing")
	}

	// Validate image specification
	if containerMeta.Image == nil || containerMeta.Image.Image == "" {
		return errors.New("container image is required")
	}

	// Validate port mappings
	for hostPort, containerPort := range containerMeta.Ports {
		if hostPort < 1 || hostPort > 65535 {
			return fmt.Errorf("invalid host port %d: must be between 1 and 65535", hostPort)
		}
		if containerPort < 1 || containerPort > 65535 {
			return fmt.Errorf("invalid container port %d: must be between 1 and 65535", containerPort)
		}
	}

	// Validate volume mounts
	for hostPath, containerPath := range containerMeta.Mounts {
		if hostPath == "" || containerPath == "" {
			return errors.New("host path and container path cannot be empty")
		}
		// Note: Skip file existence check for now as it may not be accessible from frontend
	}

	// Validate environment variables - check for reserved names
	for envName := range containerMeta.Env {
		if strings.HasPrefix(envName, "CRANE_") {
			log.Warnf("Environment variable %s uses reserved CRANE_ prefix", envName)
		}
	}

	return nil
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
		return nil
	} else {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Container task submission failed: %s", util.ErrMsg(reply.GetCode())),
		}
	}
}
