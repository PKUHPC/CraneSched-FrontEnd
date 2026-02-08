package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	FlagPod        bool
	FlagPodName    string
	FlagPodPorts   []string
	FlagPodUser    string
	FlagPodUserns  bool
	FlagPodHostNet bool
)

type podOptions struct {
	name    string
	ports   []string
	user    string
	userns  bool
	hostNet bool
	dns     []string
}

const kPodGateFlag = "pod"

var kPodFlagMap = map[string]bool{
	"pod-name":         true,
	"pod-port":         true,
	"pod-user":         true,
	"pod-userns":       true,
	"pod-host-network": true,
	"pod-dns":          true,
}

func initPodFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&FlagPod, kPodGateFlag, false, "Submit as container enabled job (pod will be created)")
	cmd.Flags().StringVar(&FlagPodName, "pod-name", "", "Name of pod (defaults to job name)")
	cmd.Flags().StringSliceVar(&FlagPodPorts, "pod-port", []string{}, "Publish pod port(s) in HOST:CONTAINER or PORT form")
	cmd.Flags().StringVar(&FlagPodUser, "pod-user", "", "Run pod as UID[:GID] (default: current user when --pod-userns=false)")
	cmd.Flags().BoolVar(&FlagPodUserns, "pod-userns", true, "Enable pod user namespace")
	cmd.Flags().BoolVar(&FlagPodHostNet, "pod-host-network", false, "Use host network namespace for the pod")
	cmd.Flags().StringSliceVar(&FlagDns, "pod-dns", []string{}, "Configure DNS server(s) for pod (comma-separated or repeated)")
}

func isPodJob(cmd *cobra.Command, args []CbatchArg) (bool, error) {
	podFlagPresented := false
	podGatePresented := FlagPod

	// Check if pod-related flags in script are presented
	for _, v := range args {
		// Prune leading dashes before lookup
		pruned, _ := strings.CutPrefix(v.name, "--")
		pruned, _ = strings.CutPrefix(pruned, "-")

		_, ok := kPodFlagMap[pruned]
		podFlagPresented = podFlagPresented || ok
		if pruned == kPodGateFlag {
			podGatePresented = true
		}
	}

	// Check if pod-related flags in CLI are presented
	for k := range kPodFlagMap {
		if cmd.Flags().Changed(k) {
			podFlagPresented = true
			break
		}
	}

	if podFlagPresented && !podGatePresented {
		return false, fmt.Errorf("--%s must be set when using pod-related flags", kPodGateFlag)
	}

	return podGatePresented, nil
}

func overridePodFromFlags(cmd *cobra.Command, podOpts *podOptions) {
	if cmd.Flags().Changed("pod-name") {
		podOpts.name = FlagPodName
	}
	if cmd.Flags().Changed("pod-port") {
		podOpts.ports = FlagPodPorts
	}
	if cmd.Flags().Changed("pod-user") {
		podOpts.user = FlagPodUser
	}
	if cmd.Flags().Changed("pod-userns") {
		podOpts.userns = FlagPodUserns
	}
	if cmd.Flags().Changed("pod-host-network") {
		podOpts.hostNet = FlagPodHostNet
	}
	if cmd.Flags().Changed("pod-dns") {
		podOpts.dns = FlagDns
	}
}

func parsePodUser(userSpec string, podMeta *protos.PodTaskAdditionalMeta) error {
	parts := strings.SplitN(userSpec, ":", 2)

	uid, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return fmt.Errorf("user name resolution not supported, please provide a numeric UID")
	}
	podMeta.RunAsUser = uint32(uid)

	if len(parts) == 2 {
		gid, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("group name resolution not supported, please provide a numeric GID")
		}
		podMeta.RunAsGroup = uint32(gid)
	}
	return nil
}

func parsePodPortMapping(portSpec string, ports *[]*protos.PodTaskAdditionalMeta_PortMapping) error {
	parts := strings.SplitN(portSpec, ":", 2)
	mapping := &protos.PodTaskAdditionalMeta_PortMapping{
		Protocol: protos.PodTaskAdditionalMeta_PortMapping_TCP,
	}

	if len(parts) == 1 {
		port, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid port number: %v", err)
		}
		mapping.HostPort = int32(port)
		mapping.ContainerPort = int32(port)
	} else {
		hostPort, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid host port: %v", err)
		}
		containerPort, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid container port: %v", err)
		}
		mapping.HostPort = int32(hostPort)
		mapping.ContainerPort = int32(containerPort)
	}

	if mapping.HostPort < 1 || mapping.HostPort > 65535 || mapping.ContainerPort < 1 || mapping.ContainerPort > 65535 {
		return fmt.Errorf("invalid port mapping: %d:%d", mapping.HostPort, mapping.ContainerPort)
	}

	*ports = append(*ports, mapping)
	return nil
}

func validatePodMeta(task *protos.TaskToCtld, meta *protos.PodTaskAdditionalMeta) error {
	if task.Uid != 0 && !meta.Userns {
		if meta.RunAsUser != task.Uid || meta.RunAsGroup != task.Gid {
			return fmt.Errorf("with --pod-userns=false, only current user and accessible groups are allowed")
		}
	}

	return nil
}

func buildPodMeta(task *protos.TaskToCtld, podOpts *podOptions) (*protos.PodTaskAdditionalMeta, error) {
	podMeta := &protos.PodTaskAdditionalMeta{
		Name:      podOpts.name,
		Namespace: &protos.PodTaskAdditionalMeta_NamespaceOption{},
		Userns:    podOpts.userns,
	}

	if podMeta.Name == "" {
		podMeta.Name = task.Name
	}

	if podOpts.user != "" {
		if err := parsePodUser(podOpts.user, podMeta); err != nil {
			return nil, err
		}
	} else if !podMeta.Userns {
		podMeta.RunAsUser = uint32(os.Getuid())
		podMeta.RunAsGroup = uint32(os.Getgid())
	}

	for _, port := range podOpts.ports {
		if err := parsePodPortMapping(port, &podMeta.Ports); err != nil {
			return nil, err
		}
	}

	if podOpts.hostNet {
		podMeta.Namespace.Network = protos.PodTaskAdditionalMeta_NODE
	}

	for i, server := range podOpts.dns {
		podOpts.dns[i] = strings.TrimSpace(server)
		if err := util.CheckIpv4Format(podOpts.dns[i]); err != nil {
			return nil, fmt.Errorf("invalid dns server '%s': %w", podOpts.dns[i], err)
		}
	}
	podMeta.DnsServers = podOpts.dns

	if err := validatePodMeta(task, podMeta); err != nil {
		return nil, err
	}

	return podMeta, nil
}
