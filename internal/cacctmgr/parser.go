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
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( @String | @Ident | @Number )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type DeleteCommand struct {
	Action      string           `parser:"@'delete'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( @String | @Ident | @Number )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type BlockCommand struct {
	Action      string           `parser:"@'block'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( @String | @Ident | @Number )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type UnblockCommand struct {
	Action      string           `parser:"@'unblock'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( @String | @Ident | @Number )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ModifyCommand struct {
	Action      string       `parser:"@'modify'"`
	Entity      *EntityType  `parser:"@@"`
	Where       *WhereClause `parser:"@@?"`
	Set         *SetClause   `parser:"@@?"`
	GlobalFlags []*Flag      `parser:"@@*"`
}

type ShowCommand struct {
	Action      string           `parser:"@'show'"`
	Entity      *EntityType      `parser:"@@?"`
	ID          string           `parser:"( @String | @Ident | @Number )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type EntityType struct {
	Account bool `parser:"@'account'"`
	User    bool `parser:"| @'user'"`
	Qos     bool `parser:"| @'qos'"`
}

type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' (@String | @Ident | @Number) | (@String | @Ident | @Number) )?"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Ident | @Number) | (@String | @Ident | @Number) )?"`
}

type WhereClause struct {
	Where       string           `parser:"@'where'"`
	WhereParams []*KeyValueParam `parser:"@@*"`
}

type SetParam struct {
	Key   string `parser:"@Ident"`
	Op    string `parser:"@('=' | '+=' | '-=')"`
	Value string `parser:"@(String | Ident | Number)"`
}

type SetClause struct {
	Set       string      `parser:"@'set'"`
	SetParams []*SetParam `parser:"@@*"`
}

var CAcctMgrLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `[-+]?("[^"]*"|'[^']*'|""|'')`},
	{Name: "Ident", Pattern: `[\+\-]?[a-zA-Z0-9][a-zA-Z0-9_\+\@\-\.,:\[\]T]*`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Punct", Pattern: `[-=,:]`},
})

var CAcctMgrParser = participle.MustBuild[CAcctMgrCommand](
	participle.Lexer(CAcctMgrLexer),
	participle.Elide("whitespace"),
	participle.Union[any](AddCommand{}, DeleteCommand{}, BlockCommand{}, UnblockCommand{}, ModifyCommand{}, ShowCommand{}),
)

func ParseCAcctMgrCommand(input string) (*CAcctMgrCommand, error) {
	return CAcctMgrParser.ParseString("", input)
}

func (r EntityType) String() string {
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
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetEntity() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case DeleteCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case BlockCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case UnblockCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case ModifyCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case ShowCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
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

func (c *CAcctMgrCommand) GetID() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		return cmd.ID
	case DeleteCommand:
		return cmd.ID
	case BlockCommand:
		return cmd.ID
	case UnblockCommand:
		return cmd.ID
	case ShowCommand:
		return cmd.ID
	default:
		return ""
	}
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
	default:
		return kvMap
	}

	for _, param := range params {
		kvMap[strings.ToLower(param.Key)] = param.Value
	}
	return kvMap
}

func (c *CAcctMgrCommand) GetWhereParams() map[string]string {
	whereMap := make(map[string]string)

	switch cmd := c.Command.(type) {
	case ModifyCommand:
		for _, param := range cmd.Where.WhereParams {
			whereMap[strings.ToLower(param.Key)] = param.Value
		}
	default:
		return nil
	}
	return whereMap
}

func (c *CAcctMgrCommand) GetSetParams() (map[string]string, map[string]string, map[string]string) {
	setMap := make(map[string]string)
	addMap := make(map[string]string)
	deleteMap := make(map[string]string)

	switch cmd := c.Command.(type) {
	case ModifyCommand:
		if cmd.Set == nil {
			return nil, nil, nil
		}
		for _, param := range cmd.Set.SetParams {
			if param.Op == "=" {
				setMap[param.Key] = param.Value
			} else if param.Op == "+=" {
				addMap[param.Key] = param.Value
			} else if param.Op == "-=" {
				deleteMap[param.Key] = param.Value
			}
		}
	default:
		return nil, nil, nil
	}
	return setMap, addMap, deleteMap
}

func preParseGlobalFlags(args []string) []string {
	remainingArgs := []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var flagName, flagValue string
		var hasValueInSameArg bool

		if strings.HasPrefix(arg, "-") {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg, "=", 2)
				flagName = parts[0]
				flagValue = parts[1]
				hasValueInSameArg = true
			} else {
				flagName = arg
			}
		} else {
			remainingArgs = append(remainingArgs, arg)
			continue
		}

		switch flagName {
		case "-h", "--help":
			showHelp()
			os.Exit(0)
		case "-v", "--version":
			fmt.Println(util.Version())
			os.Exit(0)
		case "-J", "--json":
			FlagJson = true
		case "-C", "--config":
			if hasValueInSameArg {
				FlagConfigFilePath = flagValue
			} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				FlagConfigFilePath = args[i+1]
				i++
			}
		case "--force":
			FlagForce = true
		default:
			remainingArgs = append(remainingArgs, arg)
		}
	}
	return remainingArgs
}
