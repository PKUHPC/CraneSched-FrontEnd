package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	metautils "CraneFrontEnd/tool/meta_cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/invoke"
	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/version"
	log "github.com/sirupsen/logrus"
)

// Action enumerates supported CNI verbs for delegate execution ordering.
type Action string

const (
	ActionAdd   Action = "ADD"
	ActionCheck Action = "CHECK"
	ActionDel   Action = "DEL"
)

// MetaPluginConf captures the JSON configuration accepted by the meta plugin.
type MetaPluginConf struct {
	cnitypes.PluginConf

	LogLevel        string           `json:"logLevel,omitempty"`
	TimeoutSeconds  int              `json:"timeoutSeconds,omitempty"`
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

	if len(conf.Delegates) == 0 {
		return nil, errors.New("meta-cni: at least one delegate is required")
	}

	for i := range conf.Delegates {
		if err := conf.Delegates[i].validate(); err != nil {
			return nil, fmt.Errorf("meta-cni: delegate %d invalid: %w", i, err)
		}
	}

	return conf, nil
}

func (d *DelegateEntry) validate() error {
	if d == nil {
		return errors.New("delegate entry is nil")
	}

	if len(d.Conf) == 0 && d.Type == "" {
		return errors.New("delegate must specify either type or conf")
	}

	return nil
}

// Execute runs delegates for a single CNI action using the provided args.
func (conf *MetaPluginConf) Execute(action Action, args *skel.CmdArgs) (cnitypes.Result, error) {
	ctx, cancel := conf.context()
	defer cancel()

	delegates := conf.inOrder(action)
	var lastResult cnitypes.Result

	for idx, delegate := range delegates {
		env, err := conf.buildRuntimeEnv(args, delegate.RuntimeOverride)
		if err != nil {
			return nil, err
		}
		restore, err := applyEnv(env)
		if err != nil {
			return nil, fmt.Errorf("meta-cni: delegate %s env setup failed: %w", delegate.identifier(), err)
		}

		log.WithFields(log.Fields{
			"delegate": delegate.identifier(),
			"action":   string(action),
			"index":    idx,
		}).Debug("invoking delegate")

		var callErr error
		switch action {
		case ActionAdd:
			var res cnitypes.Result
			res, callErr = delegate.call(ctx, action, conf.CNIVersion)
			if res != nil {
				lastResult = res
			}
		default:
			_, callErr = delegate.call(ctx, action, conf.CNIVersion)
		}

		restore()

		if callErr != nil {
			return nil, fmt.Errorf("meta-cni: delegate %s failed: %w", delegate.identifier(), callErr)
		}
	}

	return lastResult, nil
}

func (conf *MetaPluginConf) context() (context.Context, context.CancelFunc) {
	if conf.TimeoutSeconds <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), time.Duration(conf.TimeoutSeconds)*time.Second)
}

func (conf *MetaPluginConf) inOrder(action Action) []*DelegateEntry {
	if len(conf.Delegates) == 0 {
		return nil
	}

	ordered := make([]*DelegateEntry, 0, len(conf.Delegates))
	if action == ActionDel {
		for i := len(conf.Delegates) - 1; i >= 0; i-- {
			ordered = append(ordered, &conf.Delegates[i])
		}
		return ordered
	}

	for i := range conf.Delegates {
		ordered = append(ordered, &conf.Delegates[i])
	}
	return ordered
}

func (conf *MetaPluginConf) buildRuntimeEnv(args *skel.CmdArgs, delegateOverride *RuntimeOverride) (map[string]string, error) {
	env := map[string]string{
		"CNI_CONTAINERID": args.ContainerID,
		"CNI_NETNS":       args.Netns,
		"CNI_IFNAME":      args.IfName,
		"CNI_ARGS":        args.Args,
		"CNI_PATH":        args.Path,
	}

	apply := func(override *RuntimeOverride) error {
		if override == nil {
			return nil
		}
		if override.ContainerID != "" {
			env["CNI_CONTAINERID"] = override.ContainerID
		}
		if override.NetNS != "" {
			env["CNI_NETNS"] = override.NetNS
		}
		if override.IfName != "" {
			env["CNI_IFNAME"] = override.IfName
		}
		if override.CNIPath != "" {
			env["CNI_PATH"] = override.CNIPath
		}
		if len(override.Args) > 0 {
			merged, err := mergeArgs(env["CNI_ARGS"], override.Args)
			if err != nil {
				return fmt.Errorf("meta-cni: invalid args override: %w", err)
			}
			env["CNI_ARGS"] = merged
		}
		if len(override.Envs) > 0 {
			if err := mergeEnvs(env, override.Envs); err != nil {
				return fmt.Errorf("meta-cni: invalid env override: %w", err)
			}
		}
		return nil
	}

	if err := apply(conf.RuntimeOverride); err != nil {
		return nil, err
	}
	if err := apply(delegateOverride); err != nil {
		return nil, err
	}

	return env, nil
}

func applyEnv(env map[string]string) (func(), error) {
	snapshot := make(map[string]*string, len(env))

	for key, value := range env {
		if _, recorded := snapshot[key]; !recorded {
			if prev, ok := os.LookupEnv(key); ok {
				val := prev
				snapshot[key] = &val
			} else {
				snapshot[key] = nil
			}
		}

		var err error
		if value == "" {
			err = os.Unsetenv(key)
		} else {
			err = os.Setenv(key, value)
		}

		if err != nil {
			restoreEnv(snapshot)
			return nil, fmt.Errorf("set env %s: %w", key, err)
		}
	}

	return func() {
		restoreEnv(snapshot)
	}, nil
}

func restoreEnv(snapshot map[string]*string) {
	for key, value := range snapshot {
		if value == nil {
			_ = os.Unsetenv(key)
			continue
		}
		_ = os.Setenv(key, *value)
	}
}

func (d *DelegateEntry) call(ctx context.Context, action Action, cniVersion string) (cnitypes.Result, error) {
	confBytes, pluginType, err := d.effectiveConf(cniVersion)
	if err != nil {
		return nil, err
	}

	switch action {
	case ActionAdd:
		return invoke.DelegateAdd(ctx, pluginType, confBytes, nil)
	case ActionCheck:
		return nil, invoke.DelegateCheck(ctx, pluginType, confBytes, nil)
	case ActionDel:
		return nil, invoke.DelegateDel(ctx, pluginType, confBytes, nil)
	default:
		return nil, fmt.Errorf("unsupported action %s", action)
	}
}

func (d *DelegateEntry) effectiveConf(parentVersion string) ([]byte, string, error) {
	var (
		payload       map[string]interface{}
		err           error
		effectiveType = d.Type
	)

	if len(d.Conf) == 0 {
		if effectiveType == "" {
			return nil, "", errors.New("delegate type is required")
		}

		payload = map[string]interface{}{
			"cniVersion": parentVersion,
			"type":       effectiveType,
		}
		if d.Name != "" {
			payload["name"] = d.Name
		}
	} else {
		if err = json.Unmarshal(d.Conf, &payload); err != nil {
			return nil, "", fmt.Errorf("delegate %s config decode: %w", d.identifier(), err)
		}
		if payloadType, ok := payload["type"].(string); ok && payloadType != "" {
			effectiveType = payloadType
		}
		if effectiveType == "" {
			return nil, "", fmt.Errorf("delegate %s missing type", d.identifier())
		}
		if _, ok := payload["type"]; !ok {
			payload["type"] = effectiveType
		}
		if _, ok := payload["cniVersion"]; !ok && parentVersion != "" {
			payload["cniVersion"] = parentVersion
		}
		if d.Name != "" {
			if _, ok := payload["name"]; !ok {
				payload["name"] = d.Name
			}
		}
	}

	confBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("delegate %s marshal: %w", d.identifier(), err)
	}

	return confBytes, effectiveType, nil
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
	return "<delegate>"
}

func parseArgs(input string) map[string]string {
	result := make(map[string]string)
	for _, entry := range strings.Split(input, ";") {
		if entry == "" {
			continue
		}
		keyVal := strings.SplitN(entry, "=", 2)
		if len(keyVal) != 2 {
			continue
		}
		result[keyVal[0]] = keyVal[1]
	}
	return result
}

func mergeArgs(base string, overrides []string) (string, error) {
	if len(overrides) == 0 {
		return base, nil
	}

	current := parseArgs(base)

	expressions, err := metautils.ParseManipulators(overrides)
	if err != nil {
		return "", err
	}

	for _, expr := range expressions {
		if expr.Delete {
			delete(current, expr.Key)
			continue
		}
		current[expr.Key] = expr.Value
	}

	if len(current) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, current[key]))
	}
	return strings.Join(pairs, ";"), nil
}

func mergeEnvs(env map[string]string, entries []string) error {
	if len(entries) == 0 {
		return nil
	}

	expressions, err := metautils.ParseManipulators(entries)
	if err != nil {
		return err
	}

	for _, expr := range expressions {
		if expr.Delete {
			delete(env, expr.Key)
			continue
		}
		env[expr.Key] = expr.Value
	}
	return nil
}
