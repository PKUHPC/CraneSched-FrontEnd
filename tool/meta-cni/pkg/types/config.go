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

// ResultMode controls how delegate results are passed along the chain.
type ResultMode string

const (
	ResultModeNone    ResultMode = "none"
	ResultModeChained ResultMode = "chained"
	ResultModeMerged  ResultMode = "merged"
)

// MetaPluginConf captures the JSON configuration accepted by the meta plugin.
type MetaPluginConf struct {
	cnitypes.PluginConf

	LogLevel        string           `json:"logLevel,omitempty"`
	TimeoutSeconds  int              `json:"timeoutSeconds,omitempty"`
	ResultMode      ResultMode       `json:"resultMode,omitempty"`
	RuntimeOverride *RuntimeOverride `json:"runtimeOverride,omitempty"`
	Delegates       []DelegateEntry  `json:"delegates"`
}

// DelegateEntry describes a child plugin invoked by this meta plugin.
type DelegateEntry struct {
	Name            string            `json:"name,omitempty"`
	Type            string            `json:"type,omitempty"`
	Conf            json.RawMessage   `json:"conf,omitempty"`
	RuntimeOverride *RuntimeOverride  `json:"runtimeOverride,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"` // Reserved
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
	if err := conf.normalizeResultMode(); err != nil {
		return err
	}
	if len(conf.Delegates) == 0 {
		return errors.New("meta-cni: at least one delegate is required")
	}
	for i := range conf.Delegates {
		if err := conf.Delegates[i].validate(); err != nil {
			return fmt.Errorf("meta-cni: delegate %d invalid: %w", i, err)
		}
	}
	return nil
}

func (conf *MetaPluginConf) normalizeResultMode() error {
	raw := strings.TrimSpace(string(conf.ResultMode))
	if raw == "" {
		conf.ResultMode = ResultModeNone
		return nil
	}

	mode := ResultMode(strings.ToLower(raw))
	switch mode {
	case ResultModeNone, ResultModeChained, ResultModeMerged:
		conf.ResultMode = mode
		return nil
	default:
		return fmt.Errorf("meta-cni: invalid resultMode %q", conf.ResultMode)
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
		log.Warnf("meta-cni: delegate %s has no conf; using generated minimal config", d.identifier())
	}

	if d.Name == "" {
		hasName, err := d.confHasName()
		if err != nil {
			return fmt.Errorf("delegate %s config decode: %w", d.identifier(), err)
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

func (d *DelegateEntry) identifier() string {
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
