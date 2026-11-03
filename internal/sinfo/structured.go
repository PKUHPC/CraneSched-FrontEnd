// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

// These v0.0.45 types define both the data report and its runtime schema.
// Declaration order is output order. Descriptions and enums are public interface
// metadata. Removed fields are deliberately omitted from both representations.
// An int32 schema annotation must not truncate a Crane resource count.
type reportInt32 uint64

type reportNumber[T ~uint64] struct {
	Set      bool `json:"set" description:"True if number has been set; False if number is unset"`
	Infinite bool `json:"infinite" description:"True if number has been set to infinite; \"set\" and \"number\" will be ignored"`
	Number   T    `json:"number" description:"If \"set\" is True the number will be set with value; otherwise ignore number contents"`
}

func (reportNumber[T]) schemaRequired() []string { return []string{} }

func (n reportNumber[T]) complexValue() any {
	if n.Infinite {
		return "Infinity"
	}
	if !n.Set {
		return nil
	}
	return uint64(n.Number)
}

func reportNumeric[T ~uint64](value uint64, unset, infinite bool) reportNumber[T] {
	if unset || infinite {
		value = 0
	}
	return reportNumber[T]{Set: !unset && !infinite, Infinite: infinite, Number: T(value)}
}

type reportSinfoNode struct {
	State []string `json:"state" description:"Node state(s)" enum:"INVALID|UNKNOWN|DOWN|IDLE|ALLOCATED|ERROR|MIXED|FUTURE|EXTERNAL|RESERVED|UNDRAIN|CLOUD|RESUME|DRAIN|COMPLETING|NOT_RESPONDING|POWERED_DOWN|FAIL|POWERING_UP|MAINTENANCE|REBOOT_REQUESTED|REBOOT_CANCELED|POWERING_DOWN|DYNAMIC_FUTURE|REBOOT_ISSUED|PLANNED|INVALID_REG|POWER_DOWN|POWER_UP|POWER_DRAIN|POWER_DOWN_ASAP|POWER_DOWN_FORCE|DYNAMIC_NORM|BLOCKED"`
}

type reportSinfoNodes struct {
	Allocated reportInt32 `json:"allocated" description:"Number of nodes that are allocated"`
	Idle      reportInt32 `json:"idle" description:"Number of nodes that are idle"`
	Other     reportInt32 `json:"other" description:"Number of nodes that are not allocated or idle"`
	Total     reportInt32 `json:"total" description:"Total number of nodes"`
	Hostnames []string    `json:"hostnames" description:"NodeHost - List of node hostnames"`
	Addresses []string    `json:"addresses" description:"NodeAddr - List of node communication addresses"`
	Nodes     []string    `json:"nodes" description:"NodeList - List of node names"`
}

type reportSinfoCPUsLoad struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum CPUsLoad on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum CPUsLoad on any single node"`
}

type reportSinfoCPUsPerNode struct {
	Max reportNumber[reportInt32] `json:"max" description:"MaxCPUsPerNode - Maximum number of CPUs on any node available to all jobs from this partition (32 bit integer number with flags)"`
}

type reportSinfoCPUs struct {
	Allocated reportInt32            `json:"allocated" description:"Number of allocated CPUs"`
	Idle      reportInt32            `json:"idle" description:"Number of idle CPUs"`
	Other     reportInt32            `json:"other" description:"Number of CPUs that are not allocated or idle"`
	Total     reportInt32            `json:"total" description:"Total number of CPUs"`
	Minimum   reportInt32            `json:"minimum" description:"Minimum number of CPUs on any single node"`
	Maximum   reportInt32            `json:"maximum" description:"Maximum number of CPUs on any single node"`
	Load      reportSinfoCPUsLoad    `json:"load"`
	PerNode   reportSinfoCPUsPerNode `json:"per_node"`
}

type reportSinfoSockets struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum number of sockets on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum number of sockets on any single node"`
}

type reportSinfoCores struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum number of cores on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum number of cores on any single node"`
}

type reportSinfoThreads struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum number of threads per core on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum number of threads per core on any single node"`
}

type reportSinfoDisk struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum TMP_DISK on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum TMP_DISK on any single node"`
}

type reportSinfoMemoryFree struct {
	Minimum reportNumber[uint64] `json:"minimum" description:"Minimum FreeMem in megabytes on any single node (64 bit integer number with flags)"`
	Maximum reportNumber[uint64] `json:"maximum" description:"Maximum FreeMem in megabytes on any single node (64 bit integer number with flags)"`
}

type reportSinfoMemory struct {
	Minimum   uint64                `json:"minimum" description:"Minimum memory in megabytes on any single node"`
	Maximum   uint64                `json:"maximum" description:"Maximum memory in megabytes on any single node"`
	Free      reportSinfoMemoryFree `json:"free"`
	Allocated uint64                `json:"allocated" description:"AllocMem - Total memory in megabytes allocated to jobs"`
}

type reportSinfoWeight struct {
	Minimum reportInt32 `json:"minimum" description:"Minimum Weight on any single node"`
	Maximum reportInt32 `json:"maximum" description:"Maximum Weight on any single node"`
}

type reportSinfoFeatures struct {
	Total  string `json:"total" description:"Features - Features available on the nodes"`
	Active string `json:"active" description:"features_act - Features currently active on the nodes"`
}

type reportSinfoGRES struct {
	Total string `json:"total" description:"Gres - Generic resources configured on these nodes"`
	Used  string `json:"used" description:"GresUsed - Generic resources allocated to jobs"`
}

type reportSinfoReason struct {
	Description string `json:"description" description:"Why a node is unavailable"`
	Time        int64  `json:"time" description:"When the reason was set (UNIX timestamp)"`
}

type reportSinfoResourceSpec struct {
	CPUs   string `json:"cpus" description:"CpuSpecList - Comma-separated list of Slurm abstract CPU IDs reserved for system use"`
	Memory uint64 `json:"memory" description:"MemSpecLimit - Amount of RealMemory in megabytes reserved for system use"`
}

type reportSinfoPartitionNodes struct {
	AllowedAllocation string      `json:"allowed_allocation" description:"AllocNodes - Comma-separated list of nodes from which users can submit jobs in the partition"`
	Configured        string      `json:"configured" description:"Nodes - Comma-separated list of nodes which are associated with this partition"`
	Total             reportInt32 `json:"total" description:"TotalNodes - Number of nodes available in this partition (read-only)"`
}

type reportSinfoPartitionAccounts struct {
	Allowed string `json:"allowed" description:"AllowAccounts - Comma-separated list of accounts which may execute jobs in the partition"`
	Deny    string `json:"deny" description:"DenyAccounts - Comma-separated list of accounts which may not execute jobs in the partition"`
}

type reportSinfoPartitionGroups struct {
	Allowed string `json:"allowed" description:"AllowGroups - Comma-separated list of group names which may execute jobs in this partition"`
}

type reportSinfoPartitionQoS struct {
	Allowed  string `json:"allowed" description:"AllowQOS - Comma-separated list of Qos which may execute jobs in the partition"`
	Deny     string `json:"deny" description:"DenyQOS - Comma-separated list of Qos which may not execute jobs in the partition"`
	Assigned string `json:"assigned" description:"QOS - QOS name containing limits that will apply to all jobs in this partition"`
}

type reportSinfoPartitionTRES struct {
	BillingWeights string `json:"billing_weights" description:"TRESBillingWeights - Billing weights of each tracked TRES type that will be used in calculating the usage of a job"`
	Configured     string `json:"configured" description:"TRES - Number of each applicable TRES type available in this partition (read-only)"`
}

type reportSinfoPartitionCPUs struct {
	TaskBinding []string    `json:"task_binding" description:"CpuBind - Default method controlling how tasks are bound to allocated resources" enum:"CPU_BIND_TO_THREADS|CPU_BIND_TO_CORES|CPU_BIND_TO_SOCKETS|CPU_BIND_TO_LDOMS|CPU_BIND_NONE|CPU_BIND_OFF|VERBOSE"`
	Total       reportInt32 `json:"total" description:"TotalCPUs - Number of CPUs available in this partition (read-only)"`
}

type reportSinfoPartitionDefaults struct {
	PartitionMemoryPerCPU  reportNumber[uint64]      `json:"partition_memory_per_cpu" description:"DefMemPerCPU - Default real memory size available per allocated CPU in megabytes (64 bit integer number with flags)"`
	PartitionMemoryPerNode reportNumber[uint64]      `json:"partition_memory_per_node" description:"DefMemPerNode - Default real memory size available per allocated node in megabytes (64 bit integer number with flags)"`
	Time                   reportNumber[reportInt32] `json:"time" description:"DefaultTime - Run time limit in minutes used for jobs that don't specify a value (32 bit integer number with flags)"`
	Job                    string                    `json:"job" description:"JobDefaults - Comma-separated list of job default values (this field is only used to set new defaults)"`
}

type reportSinfoPartitionPartition struct {
	Exclusive     string   `json:"exclusive" description:"Exclusive= string (same as scontrol show partition)"`
	Oversubscribe string   `json:"oversubscribe" description:"OverSubscribe display: FORCE:n, NO, or YES:n (same as show partition and sinfo) (Partition oversubscribe value)"`
	State         []string `json:"state" description:"Current state(s)" enum:"INACTIVE|UNKNOWN|UP|DOWN|DRAIN"`
}

type reportSinfoPartitionMaximums struct {
	CPUsPerNode            reportNumber[reportInt32] `json:"cpus_per_node" description:"MaxCPUsPerNode - Maximum number of CPUs on any node available to all jobs from this partition (32 bit integer number with flags)"`
	CPUsPerSocket          reportNumber[reportInt32] `json:"cpus_per_socket" description:"MaxCPUsPerSocket - Maximum number of CPUs on any node available on the all jobs from this partition (32 bit integer number with flags)"`
	PartitionMemoryPerCPU  reportNumber[uint64]      `json:"partition_memory_per_cpu" description:"MaxMemPerCPU - Maximum real memory size available per allocated CPU in megabytes (64 bit integer number with flags)"`
	PartitionMemoryPerNode reportNumber[uint64]      `json:"partition_memory_per_node" description:"MaxMemPerNode - Maximum real memory size available per allocated node in a job allocation in megabytes (64 bit integer number with flags)"`
	Nodes                  reportNumber[reportInt32] `json:"nodes" description:"MaxNodes - Maximum count of nodes which may be allocated to any single job (32 bit integer number with flags)"`
	Time                   reportNumber[reportInt32] `json:"time" description:"MaxTime - Maximum run time limit for jobs in minutes (32 bit integer number with flags)"`
	OverTimeLimit          reportNumber[reportInt32] `json:"over_time_limit" description:"OverTimeLimit - Number of minutes by which a job can exceed its time limit before being canceled (16 bit integer number with flags)"`
}

type reportSinfoPartitionMinimums struct {
	Nodes reportInt32 `json:"nodes" description:"MinNodes - Minimum count of nodes which may be allocated to any single job"`
}

type reportSinfoPartitionPriority struct {
	JobFactor reportInt32 `json:"job_factor" description:"PriorityJobFactor - Partition factor used by priority/multifactor plugin in calculating job priority"`
	Tier      reportInt32 `json:"tier" description:"PriorityTier - Controls the order in which the scheduler evaluates jobs from different partitions"`
}

type reportSinfoPartitionTimeouts struct {
	Resume  reportNumber[reportInt32] `json:"resume" description:"ResumeTimeout - Resumed nodes which fail to respond in this time frame will be marked DOWN (read-only; GLOBAL if both set and infinite are false). (16 bit integer number with flags)"`
	Suspend reportNumber[reportInt32] `json:"suspend" description:"SuspendTimeout - Maximum time permitted (in seconds) between when a node suspend request is issued and when the node is shutdown (read-only; GLOBAL if both set and infinite are false). (16 bit integer number with flags)"`
}

type reportSinfoPartition struct {
	Nodes       reportSinfoPartitionNodes     `json:"nodes"`
	Accounts    reportSinfoPartitionAccounts  `json:"accounts"`
	Groups      reportSinfoPartitionGroups    `json:"groups"`
	QoS         reportSinfoPartitionQoS       `json:"qos"`
	Alternate   string                        `json:"alternate" description:"Alternate - Partition name of alternate partition to be used if the state of this partition is DRAIN or INACTIVE"`
	TRES        reportSinfoPartitionTRES      `json:"tres"`
	Cluster     string                        `json:"cluster" description:"Cluster name (read-only)"`
	SelectType  []string                      `json:"select_type" description:"Scheduler consumable resource selection type (read-only)" enum:"NONE|CR_CPU_MEMORY|CR_CPU|CR_CORE_MEMORY|CR_CORE|CR_SOCKET_MEMORY|CR_SOCKET|CR_MEMORY|CR_ONE_TASK_PER_CORE|CR_CORE_DEFAULT_DIST_BLOCK|CR_NO_DIST_TOPO_BLOCK|CR_LLN|CR_PACK_NODES|LL_SHARED_GRES|MULTIPLE_SHARING_GRES_PJ|ENFORCE_BINDING_GRES|ONE_TASK_PER_SHARING_GRES"`
	CPUs        reportSinfoPartitionCPUs      `json:"cpus"`
	Defaults    reportSinfoPartitionDefaults  `json:"defaults"`
	Flags       []string                      `json:"flags" description:"Partition flag options" enum:"DEFAULT|HIDDEN|NO_ROOT|ROOT_ONLY|REQ_RESV|LLN|PDOI|DEFAULT_CLEAR|HIDDEN_CLEAR|NO_ROOT_CLEAR|ROOT_ONLY_CLEAR|REQ_RESV_CLEAR|LLN_CLEAR|PDOI_CLEAR"`
	Partition   reportSinfoPartitionPartition `json:"partition"`
	GraceTime   reportInt32                   `json:"grace_time" description:"GraceTime - Grace time in seconds to be extended to a job which has been selected for preemption"`
	Maximums    reportSinfoPartitionMaximums  `json:"maximums"`
	Minimums    reportSinfoPartitionMinimums  `json:"minimums"`
	Name        string                        `json:"name" description:"PartitionName - Name by which the partition may be referenced"`
	NodeSets    string                        `json:"node_sets" description:"NodeSets - Comma-separated list of nodesets which are associated with this partition"`
	PreemptMode []string                      `json:"preempt_mode" description:"PreemptMode - Mechanism used to preempt jobs for this partition when PreemptType=preempt/partition_prio is configured (the \"CLUSTER_GLOBAL\" flag will be ignored when updating a partition)." enum:"CLUSTER_GLOBAL|DISABLED|SUSPEND|REQUEUE|CANCEL|PRIORITY"`
	Priority    reportSinfoPartitionPriority  `json:"priority"`
	Timeouts    reportSinfoPartitionTimeouts  `json:"timeouts"`
	Topology    string                        `json:"topology" description:"Topology - Name of the topology, defined in topology.yaml, used by jobs in this partition"`
	SuspendTime reportNumber[reportInt32]     `json:"suspend_time" description:"SuspendTime - Nodes which remain idle or down for this number of seconds will be placed into power save mode (read-only; GLOBAL if both set and infinite are false). (32 bit integer number with flags)"`
}

type reportSinfo struct {
	Port            reportInt32             `json:"port" description:"Node TCP port"`
	Node            reportSinfoNode         `json:"node"`
	Nodes           reportSinfoNodes        `json:"nodes"`
	CPUs            reportSinfoCPUs         `json:"cpus"`
	Sockets         reportSinfoSockets      `json:"sockets"`
	Cores           reportSinfoCores        `json:"cores"`
	Threads         reportSinfoThreads      `json:"threads"`
	Disk            reportSinfoDisk         `json:"disk"`
	Memory          reportSinfoMemory       `json:"memory"`
	Weight          reportSinfoWeight       `json:"weight"`
	Features        reportSinfoFeatures     `json:"features"`
	GRES            reportSinfoGRES         `json:"gres"`
	Cluster         string                  `json:"cluster" description:"Cluster name if running in a federation"`
	Comment         string                  `json:"comment" description:"Arbitrary descriptive string"`
	Extra           string                  `json:"extra" description:"Arbitrary string used for node filtering if SchedulerParameters=extra_constraints is enabled"`
	Reason          reportSinfoReason       `json:"reason"`
	Reservation     string                  `json:"reservation" description:"Name of advanced reservation"`
	ReasonSetByUser string                  `json:"reason_set_by_user" description:"User who set the reason" complex:"keep_empty"`
	ResourceSpec    reportSinfoResourceSpec `json:"resource_spec"`
	Partition       reportSinfoPartition    `json:"partition" description:"Partition name; default partition is followed by \"*\""`
}

type reportMetaPlugin struct {
	Type              string `json:"type" description:"Slurm plugin type (if applicable)"`
	Name              string `json:"name" description:"Slurm plugin name (if applicable)"`
	DataParser        string `json:"data_parser" description:"Slurm data_parser plugin"`
	AccountingStorage string `json:"accounting_storage" description:"Slurm accounting plugin"`
}

type reportMetaClient struct {
	Source string `json:"source" description:"Client source description"`
	User   string `json:"user" description:"Client user (if known)"`
	Group  string `json:"group" description:"Client group (if known)"`
}

type reportMetaSlurmVersion struct {
	Major string `json:"major" description:"Slurm release major version"`
	Micro string `json:"micro" description:"Slurm release micro version"`
	Minor string `json:"minor" description:"Slurm release minor version"`
}

type reportMetaSlurm struct {
	Version reportMetaSlurmVersion `json:"version"`
	Release string                 `json:"release" description:"Slurm release string"`
	Cluster string                 `json:"cluster" description:"Slurm cluster name"`
}

type reportMeta struct {
	Plugin  reportMetaPlugin `json:"plugin"`
	Client  reportMetaClient `json:"client"`
	Command []string         `json:"command" description:"CLI command (if applicable)"`
	Slurm   reportMetaSlurm  `json:"slurm"`
}

type reportErrors struct {
	Description string      `json:"description" description:"Long form error description"`
	ErrorNumber reportInt32 `json:"error_number" description:"Slurm numeric error identifier"`
	Error       string      `json:"error" description:"Short form error description"`
	Source      string      `json:"source" description:"Source of error or where error was first detected"`
}

type reportWarnings struct {
	Description string `json:"description" description:"Long form warning description"`
	Source      string `json:"source" description:"Source of warning or where warning was first detected"`
}

type reportResponse struct {
	Sinfo    []reportSinfo    `json:"sinfo" description:"node and partition info"`
	Meta     reportMeta       `json:"meta" description:"Slurm meta values"`
	Errors   []reportErrors   `json:"errors" description:"Query errors"`
	Warnings []reportWarnings `json:"warnings" description:"Query warnings"`
}

func (reportSinfoPartition) schemaRequired() []string { return []string{} }
func (reportSinfo) schemaRequired() []string          { return []string{} }
func (reportMeta) schemaRequired() []string           { return []string{} }
func (reportErrors) schemaRequired() []string         { return []string{} }
func (reportWarnings) schemaRequired() []string       { return []string{} }
func (reportResponse) schemaRequired() []string       { return []string{"sinfo"} }
