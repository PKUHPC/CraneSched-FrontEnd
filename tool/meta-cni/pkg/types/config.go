package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/version"
	log "github.com/sirupsen/logrus"
)

// MetaPluginConf captures the JSON configuration accepted by the meta plugin.
type MetaPluginConf struct {
	cnitypes.PluginConf

	LogLevel        string           `json:"logLevel,omitempty"`
	TimeoutSeconds  int              `json:"timeoutSeconds,omitempty"`
	RuntimeOverride *RuntimeOverride `json:"runtimeOverride,omitempty"`
	Pipelines       []Pipeline       `json:"pipelines"`

	// RuntimeConfig is passed by the container runtime (e.g., containerd)
	// and contains dynamic settings like port mappings, bandwidth limits, etc.
	RuntimeConfig map[string]any `json:"runtimeConfig,omitempty"`
}

// Pipeline describes an independent CNI plugin chain corresponding to one
// network interface inside the container.
//
// Exactly one of IfName or IfNamePrefix must be set:
//   - IfName set     → Static Pipeline (always executed, fixed interface name)
//   - IfNamePrefix set → Template Pipeline (expanded by GRES annotations)
type Pipeline struct {
	Name            string           `json:"name"`
	IfName          string           `json:"ifName,omitempty"`
	IfNamePrefix    string           `json:"ifNamePrefix,omitempty"`
	RuntimeOverride *RuntimeOverride `json:"runtimeOverride,omitempty"`
	Delegates       []DelegateEntry  `json:"delegates"`
}

// IsTemplate returns true if this pipeline is a template pipeline.
func (p *Pipeline) IsTemplate() bool {
	return p.IfNamePrefix != ""
}

// DelegateEntry describes a child plugin invoked by this meta plugin.
type DelegateEntry struct {
	Name            string            `json:"name,omitempty"`
	Type            string            `json:"type,omitempty"`
	Conf            json.RawMessage   `json:"conf,omitempty"`
	RuntimeOverride *RuntimeOverride  `json:"runtimeOverride,omitempty"`
	ConfFromArgs    map[string]string `json:"confFromArgs,omitempty"`
}

// RuntimeOverride alters the runtime information passed to child plugins.
type RuntimeOverride struct {
	ContainerID string   `json:"containerID,omitempty"`
	NetNS       string   `json:"netns,omitempty"`
	IfName      string   `json:"ifName,omitempty"`
	CNIPath     string   `json:"cniPath,omitempty"`
	Args        []string `json:"args,omitempty"`
	Envs        []string `json:"envs,omitempty"`
}

// LoadMetaPluginConf decodes plugin configuration from stdin bytes.
func LoadMetaPluginConf(data []byte) (*MetaPluginConf, error) {
	conf := &MetaPluginConf{}
	if err := json.Unmarshal(data, conf); err != nil {
		return nil, fmt.Errorf("meta-cni: decode config: %w", err)
	}

	if conf.CNIVersion == "" {
		conf.CNIVersion = version.Current()
	}

	if err := version.ParsePrevResult(&conf.PluginConf); err != nil {
		return nil, fmt.Errorf("meta-cni: parse prevResult: %w", err)
	}

	return conf, nil
}

// Validate performs config checks that should run after logger initialization.
func (conf *MetaPluginConf) Validate() error {
	if conf == nil {
		return errors.New("meta-cni: config is nil")
	}
	if conf.Name == "" {
		return errors.New("meta-cni: name is required")
	}
	if len(conf.Pipelines) == 0 {
		return errors.New("meta-cni: at least one pipeline is required")
	}

	names := make(map[string]bool, len(conf.Pipelines))
	staticIfNames := make(map[string]bool)
	templatePrefixes := make(map[string]bool)

	// First pass: collect names and ifName/ifNamePrefix.
	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]
		if p.Name == "" {
			return fmt.Errorf("meta-cni: pipeline %d: name is required", i)
		}
		if names[p.Name] {
			return fmt.Errorf("meta-cni: pipeline %q: duplicate name", p.Name)
		}
		names[p.Name] = true

		hasIfName := p.IfName != ""
		hasPrefix := p.IfNamePrefix != ""
		if hasIfName == hasPrefix {
			return fmt.Errorf("meta-cni: pipeline %q: exactly one of ifName or ifNamePrefix must be set", p.Name)
		}

		if hasIfName {
			if staticIfNames[p.IfName] {
				return fmt.Errorf("meta-cni: pipeline %q: duplicate ifName %q", p.Name, p.IfName)
			}
			staticIfNames[p.IfName] = true
		} else {
			if templatePrefixes[p.IfNamePrefix] {
				return fmt.Errorf("meta-cni: pipeline %q: duplicate ifNamePrefix %q", p.Name, p.IfNamePrefix)
			}
			templatePrefixes[p.IfNamePrefix] = true
		}
	}

	// Second pass: cross-check static ifNames vs template prefixes.
	for ifName := range staticIfNames {
		for prefix := range templatePrefixes {
			if strings.HasPrefix(ifName, prefix) {
				return fmt.Errorf("meta-cni: static ifName %q conflicts with template prefix %q", ifName, prefix)
			}
		}
	}

	// Third pass: cross-check template prefixes against each other.
	for left := range templatePrefixes {
		for right := range templatePrefixes {
			if left == right {
				continue
			}
			if strings.HasPrefix(left, right) || strings.HasPrefix(right, left) {
				return fmt.Errorf("meta-cni: template prefix %q conflicts with template prefix %q", left, right)
			}
		}
	}

	// Fourth pass: validate delegates.
	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]

		if len(p.Delegates) == 0 {
			return fmt.Errorf("meta-cni: pipeline %q: at least one delegate is required", p.Name)
		}
		for j := range p.Delegates {
			if err := p.Delegates[j].validate(); err != nil {
				return fmt.Errorf("meta-cni: pipeline %q delegate %d: %w", p.Name, j, err)
			}
		}

		if p.RuntimeOverride != nil && p.RuntimeOverride.IfName != "" {
			log.Warnf("meta-cni: pipeline %q has runtimeOverride.ifName set; "+
				"prefer using pipeline.ifName instead", p.Name)
		}
	}

	return nil
}

// Annotations extracts pod annotations from runtimeConfig.
func (conf *MetaPluginConf) Annotations() map[string]string {
	if conf.RuntimeConfig == nil {
		return nil
	}
	raw, ok := conf.RuntimeConfig["io.kubernetes.cri.pod-annotations"]
	if !ok {
		return nil
	}

	switch v := raw.(type) {
	case map[string]string:
		return v
	case map[string]any:
		out := make(map[string]string, len(v))
		for k, val := range v {
			if s, ok := val.(string); ok {
				out[k] = s
			}
		}
		return out
	default:
		return nil
	}
}

func (d *DelegateEntry) validate() error {
	if d == nil {
		return errors.New("delegate entry is nil")
	}

	if len(d.Conf) == 0 && d.Type == "" {
		return errors.New("delegate must specify either type or conf")
	}

	if len(d.Conf) == 0 {
		log.Warnf("meta-cni: delegate %s has no conf; using generated minimal config", d.Identifier())
	}

	if d.Name == "" {
		hasName, err := d.confHasName()
		if err != nil {
			return fmt.Errorf("delegate %s config decode: %w", d.Identifier(), err)
		}
		if !hasName {
			return errors.New("delegate name is required (set delegates[].name or conf.name)")
		}
	}

	return nil
}

func (d *DelegateEntry) confHasName() (bool, error) {
	if len(d.Conf) == 0 {
		return false, nil
	}

	var payload struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(d.Conf, &payload); err != nil {
		return false, err
	}
	return payload.Name != "", nil
}

// Identifier returns a stable name for logging and errors.
func (d *DelegateEntry) Identifier() string {
	if d == nil {
		return "<nil>"
	}
	if d.Name != "" {
		return d.Name
	}
	if d.Type != "" {
		return d.Type
	}
	return "<unknown>"
}

// String returns a human-readable representation of DelegateEntry for logging.
func (d DelegateEntry) String() string {
	return fmt.Sprintf("{Name:%s Type:%s Conf:%s RuntimeOverride:%+v ConfFromArgs:%v}",
		d.Name, d.Type, string(d.Conf), d.RuntimeOverride, d.ConfFromArgs)
}
