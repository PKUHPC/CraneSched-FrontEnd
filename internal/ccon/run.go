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
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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
		// We currently not intend to support user name resolution
		return fmt.Errorf("user name resolution not supported, please provide UID")
	}

	// Parse group if provided
	if len(parts) == 2 {
		group := parts[1]
		if gid, err := strconv.ParseUint(group, 10, 32); err == nil {
			containerMeta.RunAsGroup = uint32(gid)
		} else {
			return fmt.Errorf("group name resolution not supported, please provide GID")
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
		return fmt.Errorf("environment variable must be in KEY=VALUE or KEY format")
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
		return fmt.Errorf("volume mount must be in HOST:CONTAINER format")
	}

	hostPath := parts[0]
	containerPath := parts[1]

	// Basic validation
	if hostPath == "" || containerPath == "" {
		return fmt.Errorf("host path and container path cannot be empty")
	}

	mountMap[hostPath] = containerPath
	return nil
}

// runExecute handles the run command execution
func runExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "run requires at least one argument: IMAGE [COMMAND] [ARG...]")
	}

	f := GetFlags()
	image := args[0]
	var command []string
	if len(args) > 1 {
		command = args[1:]
	}

	// Build the container task
	task, err := buildContainerTask(f, image, command)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "failed to build container task: %v", err)
	}

	// Validate container-specific parameters
	if err := validateContainerTask(task); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "validation failed: %v", err)
	}

	// Submit the task
	reply, err := submitContainerTask(task)
	if f.Global.Json {
		outputJson("run", "", f.Run, reply)
		return err
	}
	if err != nil {
		return err
	}

	return attachAfterRun(f, reply)
}

// applyResourceOptions applies resource options to the task
func applyResourceOptions(f *Flags, task *protos.TaskToCtld) error {
	// Check mutally exclusive flags
	if f.Run.Cpus > 0 && f.Crane.CpusPerTask > 0 {
		return fmt.Errorf("--cpus and --cpus-per-task are mutually exclusive")
	}

	if f.Run.Memory != "" && f.Crane.Mem != "" {
		return fmt.Errorf("--memory and --mem are mutually exclusive")
	}

	if f.Run.Gpus != "" && f.Crane.Gres != "" {
		return fmt.Errorf("--gpus and --gres are mutually exclusive")
	}

	// CPU allocation
	if f.Crane.CpusPerTask > 0 {
		task.CpusPerTask = f.Crane.CpusPerTask
	} else if f.Run.Cpus > 0 {
		log.Warn("--cpus is deprecated, please use --cpus-per-task instead")
		task.CpusPerTask = f.Run.Cpus
	} else {
		// Default to 1 CPU if not specified
		task.CpusPerTask = 1
	}

	// Memory allocation - prefer --mem over --memory
	memorySpec := ""
	if f.Crane.Mem != "" {
		memorySpec = f.Crane.Mem
	} else if f.Run.Memory != "" {
		log.Warn("--memory is deprecated, please use --mem instead")
		memorySpec = f.Run.Memory
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
	if f.Crane.Gres != "" {
		gresSpec = f.Crane.Gres
	} else if f.Run.Gpus != "" {
		return fmt.Errorf("--gpus is not supported. Please use --gres instead with format like 'gpu:1' or 'gpu:a100:2'")
	}

	if gresSpec != "" {
		// Set GPU resources via DeviceMap (like cbatch does)
		task.ReqResources.DeviceMap = util.ParseGres(gresSpec)
	}

	return nil
}

// applySchedulingOptions applies cluster scheduling options to the task
func applySchedulingOptions(f *Flags, task *protos.TaskToCtld) error {
	// Partition/queue assignment
	if f.Crane.Partition != "" {
		task.PartitionName = f.Crane.Partition
	}

	// Time limit
	if f.Crane.Time != "" {
		seconds, err := util.ParseDurationStrToSeconds(f.Crane.Time)
		if err != nil {
			return fmt.Errorf("invalid time specification '%s': %v", f.Crane.Time, err)
		}
		task.TimeLimit.Seconds = seconds
	}

	// Account
	if f.Crane.Account != "" {
		task.Account = f.Crane.Account
	}

	// QoS
	if f.Crane.Qos != "" {
		task.Qos = f.Crane.Qos
	}

	// Node allocation - validate parameters
	if f.Crane.Nodes > 0 {
		task.NodeNum = f.Crane.Nodes
	} else {
		task.NodeNum = 1
	}

	if f.Crane.NtasksPerNode > 0 {
		task.NtasksPerNode = f.Crane.NtasksPerNode
	} else {
		task.NtasksPerNode = 1
	}

	// Node list (specific nodes)
	if f.Crane.Nodelist != "" {
		task.Nodelist = f.Crane.Nodelist
	}

	// Exclude nodes
	if f.Crane.Excludes != "" {
		task.Excludes = f.Crane.Excludes
	}

	// Reservation
	if f.Crane.Reservation != "" {
		task.Reservation = f.Crane.Reservation
	}

	// Extra attributes - handle both individual flags and JSON like cbatch
	structExtraFromCli := util.JobExtraAttrs{}

	if f.Crane.ExtraAttr != "" {
		structExtraFromCli.ExtraAttr = f.Crane.ExtraAttr
	}
	if f.Crane.MailType != "" {
		structExtraFromCli.MailType = f.Crane.MailType
	}
	if f.Crane.MailUser != "" {
		structExtraFromCli.MailUser = f.Crane.MailUser
	}
	if f.Crane.Comment != "" {
		structExtraFromCli.Comment = f.Crane.Comment
	}

	// Marshal extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return fmt.Errorf("invalid extra attributes: %v", err)
	}
	task.ExtraAttr = extraFromCli

	// Exclusive node allocation
	task.Exclusive = f.Crane.Exclusive

	// Hold job submission
	task.Hold = f.Crane.Hold

	return nil
}

// applyEnvironmentOptions applies environment-related options to the task
func applyEnvironmentOptions(_ *Flags, task *protos.TaskToCtld) error {
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

// applyIOOptions configures TTY and input stream options for the container
// Based on docker/podman behavior matrix:
//
// | Flags     | fg/bg | tty   | stdin | stdin_once | description                                 |
// | ------------------ | ---: | -----: | -----: | ---------: | ------------------------------------- |
// | (none -i/-d/-t)    | foreground | false | false |      false | Run in foreground; no interactive input channel |
// | -i                 | foreground | false |  true |       true | Has STDIN, one-time: input is closed when session ends |
// | -t                 | foreground |  true | false |      false | TTY mode; merged output (stderr combined); no input channel |
// | -it                | foreground |  true |  true |       true | Common interactive: TTY + one-time STDIN |
// | -d                 | background | false | false |      false | Background only; no interactive input |
// | -di                | background | false |  true |      false | Background but retains persistent input capability; can attach -i later |
// | -dt                | background |  true | false |      false | Background + TTY; no input |
// | -dit               | background |  true |  true |      false | Background + TTY + persistent input capability; can attach -it later |
func applyIOOptions(f *Flags, containerMeta *protos.ContainerTaskAdditionalMeta) {
	// TTY allocation: directly use -t flag
	containerMeta.Tty = f.Run.Tty

	// Stdin attachment: directly use -i flag
	containerMeta.Stdin = f.Run.Interactive

	// Stdin_once logic:
	// - true for foreground containers with stdin
	// - false for background containers
	// - false when stdin is disabled
	containerMeta.StdinOnce = containerMeta.Stdin && !f.Run.Detach
}

// buildContainerTask creates a TaskToCtld with container metadata from command line arguments
func buildContainerTask(f *Flags, image string, command []string) (*protos.TaskToCtld, error) {
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
	if err := applyResourceOptions(f, task); err != nil {
		return nil, fmt.Errorf("failed to apply resource options: %v", err)
	}

	// Apply cluster scheduling options
	if err := applySchedulingOptions(f, task); err != nil {
		return nil, fmt.Errorf("failed to apply scheduling options: %v", err)
	}

	// Apply environment options
	if err := applyEnvironmentOptions(f, task); err != nil {
		return nil, fmt.Errorf("failed to apply environment options: %v", err)
	}

	// Parse image reference to extract registry, repository, and tag
	registry, repository, tag := parseImageRef(image)

	// Reconstruct full image name, handling both tags and digests
	var fullImageName string
	if strings.HasPrefix(tag, "sha256:") || strings.HasPrefix(tag, "sha1:") || strings.HasPrefix(tag, "sha512:") {
		// This is a digest reference, use @ separator
		fullImageName = repository + "@" + tag
	} else {
		// This is a tag reference, use : separator
		fullImageName = repository + ":" + tag
	}

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
		Userns:   f.Run.UserNS,
		Detached: f.Run.Detach,
		Env:      make(map[string]string),
		Mounts:   make(map[string]string),
		Ports:    make(map[int32]int32),
	}

	// Set container name
	// Currently, we set both names in Job and ContainerMeta, however,
	// name in ContainerMeta may be ignored.
	if f.Run.Name != "" {
		containerMeta.Name = f.Run.Name
		task.Name = f.Run.Name
	} else {
		containerMeta.Name = ""
		task.Name = ""
	}

	// Set entrypoint and command
	if f.Run.Entrypoint != "" {
		containerMeta.Command = f.Run.Entrypoint
		containerMeta.Args = command
	} else {
		if len(command) > 0 {
			containerMeta.Command = command[0]
			if len(command) > 1 {
				containerMeta.Args = command[1:]
			}
		}
	}

	// Set working directory
	if f.Run.Workdir != "" {
		containerMeta.Workdir = f.Run.Workdir
	}

	// Parse and set user information
	if f.Run.User != "" {
		if err := parseUserSpec(f.Run.User, containerMeta); err != nil {
			return nil, fmt.Errorf("invalid user specification '%s': %v", f.Run.User, err)
		}
	} else {
		// When --user is not specified, default behavior:
		// - userns=true → run as container root (UID 0), which maps to current user
		// - userns=false → run as current user (must match task.Uid/Gid)
		if !containerMeta.Userns {
			containerMeta.RunAsUser = task.Uid
			containerMeta.RunAsGroup = task.Gid
		}
	}

	// Parse and set environment variables (container-specific)
	for _, env := range f.Run.Env {
		if err := parseEnvVar(env, containerMeta.Env); err != nil {
			return nil, fmt.Errorf("invalid environment variable '%s': %v", env, err)
		}
	}

	// Parse and set port mappings
	for _, port := range f.Run.Ports {
		if err := parsePortMapping(port, containerMeta.Ports); err != nil {
			return nil, fmt.Errorf("invalid port mapping '%s': %v", port, err)
		}
	}

	// Parse and set volume mounts
	for _, volume := range f.Run.Volume {
		if err := parseVolumeMount(volume, containerMeta.Mounts); err != nil {
			return nil, fmt.Errorf("invalid volume mount '%s': %v", volume, err)
		}
	}

	// Set TTY and stdin behavior based on container configuration
	applyIOOptions(f, containerMeta)

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
		return fmt.Errorf("container metadata is missing")
	}

	// Validate user and group IDs
	// NOTE: If UserNS is enabled, we allow running as root (UID 0)
	// because it will be mapped to the actual user outside the container.
	if task.Uid != 0 {
		if !containerMeta.Userns && (task.Uid != containerMeta.RunAsUser || task.Gid != containerMeta.RunAsGroup) {
			return fmt.Errorf("with --userns=false, only current user and accessible groups are allowed")
		}
	} else if containerMeta.Userns {
		log.Warnf("--userns is ignored when running as root")
	}

	// Validate image specification
	if containerMeta.Image == nil || containerMeta.Image.Image == "" {
		return fmt.Errorf("container image is required")
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
			return fmt.Errorf("host path and container path cannot be empty")
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
func submitContainerTask(task *protos.TaskToCtld) (*protos.SubmitBatchTaskReply, error) {
	req := &protos.SubmitBatchTaskRequest{Task: task}

	reply, err := stub.SubmitBatchTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit the container task")
		return reply, util.NewCraneErr(util.ErrorNetwork, "")
	}

	if reply.GetOk() {
		return reply, nil
	} else {
		return reply, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Container task submission failed: %s", util.ErrMsg(reply.GetCode())))
	}
}

// Handle auto-attaching to container after run command
// See applyIOOptions for explanation of the behavior matrix
func attachAfterRun(f *Flags, reply *protos.SubmitBatchTaskReply) error {
	if f.Run.Detach {
		fmt.Printf("Container task submitted successfully. Task ID: %d\n", reply.GetTaskId())
		return nil
	}

	streamOpt := StreamOptions{
		Stdin:     f.Run.Interactive,
		Stdout:    true,
		Stderr:    !f.Run.Tty,
		Tty:       f.Run.Tty,
		Transport: "spdy",
	}

	attach := func(ctx context.Context) (*protos.AttachContainerTaskReply, error) {
		req := protos.AttachContainerTaskRequest{
			TaskId: uint32(reply.GetTaskId()),
			Uid:    uint32(os.Getuid()),
			Stdin:  streamOpt.Stdin,
			Stdout: streamOpt.Stdout,
			Stderr: streamOpt.Stderr,
			Tty:    streamOpt.Tty,
		}

		grpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		reply, err := stub.AttachContainerTask(grpcCtx, &req)
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to get attach URL")
			return nil, util.NewCraneErr(util.ErrorNetwork, "")
		}

		return reply, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		sig := <-sigchan
		log.Infof("Received signal %s, exiting...\n(Note: Task is not cancelled.)", sig)
		cancel()
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}

		reply, err := attach(ctx)
		if err != nil {
			// Network or other gRPC errors: exit immediately
			return err
		}

		if reply.GetOk() {
			// Case 1: Success - execute expected operation
			return StreamWithURL(ctx, reply.GetUrl(), streamOpt)
		}

		if reply.GetStatus().GetCode() == protos.ErrCode_ERR_CRI_CONTAINER_NOT_READY {
			// Case 2: NOT_READY - retry until status changes
			log.Debugf("Attach not ready yet: %s. Retrying in 10 seconds...", reply.GetStatus().GetDescription())
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Second):
				continue
			}
		}

		// Case 3: Any other backend error - exit immediately with error
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to get attach URL: %s", reply.GetStatus().GetDescription()))
	}
}
