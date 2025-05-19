/**
 * Copyright (c) 2024 Peking University and Peking University
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

package cacctmgr

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type CAcctMgrCommand struct {
	Command any `parser:"@@"`
}

type AddCommand struct {
	Action      string           `parser:"@'add'"`
	Resource    *ResourceType    `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type DeleteCommand struct {
	Action      string           `parser:"@'delete'"`
	Resource    *ResourceType    `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type BlockCommand struct {
	Action      string           `parser:"@'block'"`
	Resource    *ResourceType    `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type UnblockCommand struct {
	Action      string           `parser:"@'unblock'"`
	Resource    *ResourceType    `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ModifyCommand struct {
	Action      string           `parser:"@'modify'"`
	Resource    *ResourceType    `parser:"@@"`
	Where       *WhereClause     `parser:"@@?"`
	WhereParams []*KeyValueParam `parser:"@@*"`
	Set         *SetClause       `parser:"@@?"`
	SetParams   []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ShowCommand struct {
	Action      string           `parser:"@'show'"`
	Resource    *ResourceType    `parser:"@@?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type FindCommand struct {
	Action      string           `parser:"@'find'"`
	Resource    *ResourceType    `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ResourceType struct {
	Account bool `parser:"@'account'"`
	User    bool `parser:"| @'user'"`
	Qos     bool `parser:"| @'qos'"`
}

type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

type WhereClause struct {
	Where       string           `parser:"@'where'"`
	WhereParams []*KeyValueParam `parser:"@@*"`
}

type SetClause struct {
	Set       string           `parser:"@'set'"`
	SetParams []*KeyValueParam `parser:"@@*"`
}

var CAcctMgrLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `"[^"]*"|'[^']*'`},
	{Name: "TimeFormat", Pattern: `\d+:\d+:\d+|\d+-\d+:\d+:\d+`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Ident", Pattern: `[a-zA-Z][a-zA-Z0-9_\-\.,]*`},
	{Name: "Punct", Pattern: `[-=:,]`},
})

var CAcctMgrParser = participle.MustBuild[CAcctMgrCommand](
	participle.Lexer(CAcctMgrLexer),
	participle.Elide("whitespace"),
	participle.Union[any](AddCommand{}, DeleteCommand{}, BlockCommand{}, UnblockCommand{}, ModifyCommand{}, ShowCommand{}, FindCommand{}),
)

func ParseCAcctMgrCommand(input string) (*CAcctMgrCommand, error) {
	return CAcctMgrParser.ParseString("", input)
}

func (r ResourceType) String() string {
	switch {
	case r.Account:
		return "account"
	case r.User:
		return "user"
	case r.Qos:
		return "qos"
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetAction() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		return cmd.Action
	case DeleteCommand:
		return cmd.Action
	case BlockCommand:
		return cmd.Action
	case UnblockCommand:
		return cmd.Action
	case ModifyCommand:
		return cmd.Action
	case ShowCommand:
		return cmd.Action
	case FindCommand:
		return cmd.Action
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetResource() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case DeleteCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case BlockCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case UnblockCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case ModifyCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case ShowCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case FindCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	}
	return ""
}

func (c *CAcctMgrCommand) GetKVParamValue(key string) string {
	var params []*KeyValueParam

	switch cmd := c.Command.(type) {
	case AddCommand:
		params = cmd.KVParams
	case DeleteCommand:
		params = cmd.KVParams
	case BlockCommand:
		params = cmd.KVParams
	case UnblockCommand:
		params = cmd.KVParams
	case ShowCommand:
		params = cmd.KVParams
	case FindCommand:
		params = cmd.KVParams
	default:
		return ""
	}

	for _, param := range params {
		if strings.EqualFold(param.Key, strings.ToLower(key)) {
			return param.Value
		}
	}
	return ""
}

func (c *CAcctMgrCommand) GetKVMaps() map[string]string {
	kvMap := make(map[string]string)
	var params []*KeyValueParam

	switch cmd := c.Command.(type) {
	case AddCommand:
		params = cmd.KVParams
	case DeleteCommand:
		params = cmd.KVParams
	case BlockCommand:
		params = cmd.KVParams
	case UnblockCommand:
		params = cmd.KVParams
	case ShowCommand:
		params = cmd.KVParams
	case FindCommand:
		params = cmd.KVParams
	default:
		return kvMap
	}

	for _, param := range params {
		kvMap[param.Key] = param.Value
	}
	return kvMap
}

func (c *CAcctMgrCommand) GetWhereParams() []*KeyValueParam {
	switch cmd := c.Command.(type) {
	case ModifyCommand:
		return cmd.WhereParams
	default:
		return nil
	}
}

func (c *CAcctMgrCommand) GetSetParams() []*KeyValueParam {
	switch cmd := c.Command.(type) {
	case ModifyCommand:
		return cmd.SetParams
	default:
		return nil
	}
}

func (c *CAcctMgrCommand) GetGlobalFlag(name string) (string, bool) {
	var flags []*Flag

	switch cmd := c.Command.(type) {
	case AddCommand:
		flags = cmd.GlobalFlags
	case DeleteCommand:
		flags = cmd.GlobalFlags
	case BlockCommand:
		flags = cmd.GlobalFlags
	case UnblockCommand:
		flags = cmd.GlobalFlags
	case ModifyCommand:
		flags = cmd.GlobalFlags
	case ShowCommand:
		flags = cmd.GlobalFlags
	case FindCommand:
		flags = cmd.GlobalFlags
	default:
		return "", false
	}

	for _, flag := range flags {
		if strings.EqualFold(flag.Name, name) {
			return flag.Value, true
		}
	}
	return "", false
}

func processGlobalFlags(command *CAcctMgrCommand) {
	_, hasJson := command.GetGlobalFlag("json")
	_, hasJ := command.GetGlobalFlag("J")
	if hasJson || hasJ {
		FlagJson = true
	}

	configFilePath, hasConfig := command.GetGlobalFlag("config")
	configFilePathShort, hasC := command.GetGlobalFlag("C")
	if hasConfig {
		FlagConfigFilePath = configFilePath
	} else if hasC {
		FlagConfigFilePath = configFilePathShort
	}

	_, hasHelp := command.GetGlobalFlag("help")
	_, hasH := command.GetGlobalFlag("h")
	if hasHelp || hasH {
		showHelp()
		os.Exit(0)
	}

	_, hasVersion := command.GetGlobalFlag("version")
	_, hasV := command.GetGlobalFlag("v")
	if hasVersion || hasV {
		fmt.Println(util.Version())
		os.Exit(0)
	}
}
