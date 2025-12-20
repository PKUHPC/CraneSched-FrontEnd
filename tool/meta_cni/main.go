package main

import (
	"fmt"

	metatypes "CraneFrontEnd/tool/meta_cni/pkg/types"
	"CraneFrontEnd/tool/meta_cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/types/create"
	"github.com/containernetworking/cni/pkg/version"
)

const (
	aboutPlugin = "CraneSched Meta CNI"
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

	result, err := conf.Execute(metatypes.ActionAdd, args)
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

	_, err = conf.Execute(metatypes.ActionCheck, args)
	return err
}

func cmdDel(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	_, err = conf.Execute(metatypes.ActionDel, args)
	return err
}

func emptyResult(cniVersion string) (cnitypes.Result, error) {
	if cniVersion == "" {
		cniVersion = version.Current()
	}
	payload := []byte(fmt.Sprintf(`{"cniVersion":%q}`, cniVersion))
	return create.Create(cniVersion, payload)
}
