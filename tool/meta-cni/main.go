package main

import (
	"fmt"
	"strings"

	"CraneFrontEnd/tool/meta-cni/pkg/engine"
	metatypes "CraneFrontEnd/tool/meta-cni/pkg/types"
	"CraneFrontEnd/tool/meta-cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/types/create"
	"github.com/containernetworking/cni/pkg/version"

	log "github.com/sirupsen/logrus"
)

const (
	aboutPlugin = `Meta CNI Plugin for CraneSched

This plugin allows chaining multiple CNI plugins and manipulating their runtime
parameters such as CNI_ARGS, Plugin Config, etc.

You can use this plugin to adapt existing CNI plugins to CraneSched.
`
)

func main() {
	skel.PluginMainFuncsWithError(
		skel.CNIFuncs{
			Add:   cmdAdd,
			Check: cmdCheck,
			Del:   cmdDel,
		},
		version.All,
		aboutPlugin,
	)
}

func cmdAdd(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Calling chained plugins in following order: [%s]", getDelegateNames(conf))

	result, err := engine.Execute(conf, engine.ActionAdd, args)
	if err != nil {
		return err
	}

	if result == nil {
		result, err = emptyResult(conf.CNIVersion)
		if err != nil {
			return err
		}
	}

	return cnitypes.PrintResult(result, conf.CNIVersion)
}

func cmdCheck(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Calling chained plugins in following order: [%s]", getDelegateNames(conf))

	_, err = engine.Execute(conf, engine.ActionCheck, args)
	return err
}

func cmdDel(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Calling chained plugins in following order: [%s]", getDelegateNames(conf))

	_, err = engine.Execute(conf, engine.ActionDel, args)
	return err
}

func getDelegateNames(conf *metatypes.MetaPluginConf) string {
	names := make([]string, len(conf.Delegates))
	for i, d := range conf.Delegates {
		if d.Name != "" {
			names[i] = d.Name
		} else if d.Type != "" {
			names[i] = d.Type
		} else {
			names[i] = "<unknown>"
		}
	}
	return strings.Join(names, ", ")
}

func emptyResult(cniVersion string) (cnitypes.Result, error) {
	if cniVersion == "" {
		cniVersion = version.Current()
	}
	payload := fmt.Appendf(nil, `{"cniVersion":%q}`, cniVersion)
	return create.Create(cniVersion, payload)
}
