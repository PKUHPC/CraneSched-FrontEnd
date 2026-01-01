package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"CraneFrontEnd/tool/meta-cni/pkg/result"
	metatypes "CraneFrontEnd/tool/meta-cni/pkg/types"
	"CraneFrontEnd/tool/meta-cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/invoke"
	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	log "github.com/sirupsen/logrus"
)

// Action enumerates supported CNI verbs for delegate execution ordering.
type Action string

const (
	ActionAdd   Action = "ADD"
	ActionCheck Action = "CHECK"
	ActionDel   Action = "DEL"
)

// Execute runs delegates for a single CNI action using the provided args.
func Execute(conf *metatypes.MetaPluginConf, action Action, args *skel.CmdArgs) (cnitypes.Result, error) {
	if conf == nil {
		return nil, errors.New("meta-cni: config is nil")
	}

	ctx, cancel := contextWithTimeout(conf.TimeoutSeconds)
	defer cancel()

	delegates := inOrder(conf.Delegates, action)
	var lastResult cnitypes.Result
	var chainResult cnitypes.Result

	for idx, delegate := range delegates {
		logger := log.WithFields(log.Fields{
			"delegate": delegateIdentifier(delegate),
			"action":   string(action),
			"index":    idx,
		})

		env, err := buildRuntimeEnv(args, conf.RuntimeOverride, delegate.RuntimeOverride)
		if err != nil {
			logger.Errorf("error in building runtime env: %v", err)
			return nil, err
		}
		restore, err := utils.ApplyEnv(env)
		if err != nil {
			logger.Errorf("error in applying runtime env: %v", err)
			return nil, fmt.Errorf("meta-cni: delegate %s env setup failed: %w", delegateIdentifier(delegate), err)
		}

		logger.Debug("invoking delegate")

		var (
			callErr      error
			latestResult cnitypes.Result
		)
		switch action {
		case ActionAdd:
			prevResult := prevResultForDelegate(conf.ResultMode, chainResult)
			latestResult, callErr = callDelegate(ctx, delegate, action, conf.CNIVersion, prevResult)
			if latestResult != nil {
				lastResult = latestResult
			}
		default:
			_, callErr = callDelegate(ctx, delegate, action, conf.CNIVersion, nil)
		}

		restore()

		if callErr == nil && action == ActionAdd {
			chainResult, callErr = updateChainResult(conf.ResultMode, chainResult, latestResult)
		}

		if callErr != nil {
			logger.Errorf("error in calling: %v", callErr)
			return nil, fmt.Errorf("meta-cni: delegate %s failed: %w", delegateIdentifier(delegate), callErr)
		}
	}

	if action == ActionAdd && conf.ResultMode == metatypes.ResultModeMerged && chainResult != nil {
		return chainResult, nil
	}

	return lastResult, nil
}

func contextWithTimeout(timeoutSeconds int) (context.Context, context.CancelFunc) {
	if timeoutSeconds <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
}

func inOrder(delegates []metatypes.DelegateEntry, action Action) []*metatypes.DelegateEntry {
	if len(delegates) == 0 {
		return nil
	}

	ordered := make([]*metatypes.DelegateEntry, 0, len(delegates))
	if action == ActionDel {
		for i := len(delegates) - 1; i >= 0; i-- {
			ordered = append(ordered, &delegates[i])
		}
		return ordered
	}

	for i := range delegates {
		ordered = append(ordered, &delegates[i])
	}
	return ordered
}

func buildRuntimeEnv(args *skel.CmdArgs, globalOverride, delegateOverride *metatypes.RuntimeOverride) (map[string]string, error) {
	env := map[string]string{
		"CNI_CONTAINERID": args.ContainerID,
		"CNI_NETNS":       args.Netns,
		"CNI_IFNAME":      args.IfName,
		"CNI_ARGS":        args.Args,
		"CNI_PATH":        args.Path,
	}

	apply := func(override *metatypes.RuntimeOverride) error {
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
			merged, err := utils.MergeArgs(env["CNI_ARGS"], override.Args)
			if err != nil {
				return fmt.Errorf("meta-cni: invalid args override: %w", err)
			}
			env["CNI_ARGS"] = merged
		}
		if len(override.Envs) > 0 {
			if err := utils.MergeEnvs(env, override.Envs); err != nil {
				return fmt.Errorf("meta-cni: invalid env override: %w", err)
			}
		}
		return nil
	}

	// Global runtime override has lower precedence than delegate-specific override.
	if err := apply(globalOverride); err != nil {
		return nil, err
	}
	if err := apply(delegateOverride); err != nil {
		return nil, err
	}

	return env, nil
}

func callDelegate(ctx context.Context, delegate *metatypes.DelegateEntry, action Action, cniVersion string, prevResult cnitypes.Result) (cnitypes.Result, error) {
	confBytes, pluginType, err := effectiveConf(delegate, cniVersion, prevResult)
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

func effectiveConf(delegate *metatypes.DelegateEntry, parentVersion string, prevResult cnitypes.Result) ([]byte, string, error) {
	if delegate == nil {
		return nil, "", errors.New("delegate entry is nil")
	}

	var (
		payload       map[string]any
		err           error
		effectiveType = delegate.Type
	)

	if len(delegate.Conf) == 0 {
		if effectiveType == "" {
			return nil, "", errors.New("delegate type is required")
		}

		payload = map[string]any{
			"cniVersion": parentVersion,
			"type":       effectiveType,
		}
		if delegate.Name != "" {
			payload["name"] = delegate.Name
		}
	} else {
		if err = json.Unmarshal(delegate.Conf, &payload); err != nil {
			return nil, "", fmt.Errorf("delegate %s config decode: %w", delegateIdentifier(delegate), err)
		}
		if payloadType, ok := payload["type"].(string); ok && payloadType != "" {
			effectiveType = payloadType
		}
		if effectiveType == "" {
			return nil, "", fmt.Errorf("delegate %s missing type", delegateIdentifier(delegate))
		}
		if _, ok := payload["type"]; !ok {
			payload["type"] = effectiveType
		}
		if _, ok := payload["cniVersion"]; !ok && parentVersion != "" {
			payload["cniVersion"] = parentVersion
		}
		if delegate.Name != "" {
			if _, ok := payload["name"]; !ok {
				payload["name"] = delegate.Name
			}
		}
	}

	if payload != nil {
		delete(payload, "prevResult")
	}

	configVersion := ""
	if rawVersion, ok := payload["cniVersion"].(string); ok {
		configVersion = rawVersion
	}

	if prevResult != nil {
		if configVersion == "" {
			return nil, "", fmt.Errorf("delegate %s missing cniVersion for prevResult", delegateIdentifier(delegate))
		}
		converted, err := prevResult.GetAsVersion(configVersion)
		if err != nil {
			return nil, "", fmt.Errorf("delegate %s prevResult convert: %w", delegateIdentifier(delegate), err)
		}
		payload["prevResult"] = converted
	}

	confBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("delegate %s marshal: %w", delegateIdentifier(delegate), err)
	}

	return confBytes, effectiveType, nil
}

func prevResultForDelegate(mode metatypes.ResultMode, chainResult cnitypes.Result) cnitypes.Result {
	if mode == metatypes.ResultModeNone {
		return nil
	}
	return chainResult
}

func updateChainResult(mode metatypes.ResultMode, current, latest cnitypes.Result) (cnitypes.Result, error) {
	switch mode {
	case metatypes.ResultModeNone:
		return nil, nil
	case metatypes.ResultModeChained:
		if latest != nil {
			return latest, nil
		}
		return current, nil
	case metatypes.ResultModeMerged:
		return result.Merge(current, latest)
	default:
		return current, nil
	}
}

func delegateIdentifier(d *metatypes.DelegateEntry) string {
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
