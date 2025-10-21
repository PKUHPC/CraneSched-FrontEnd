/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccontrol

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"CraneFrontEnd/internal/util"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type CommandAction interface {
	GetAction() string
}

type BaseAction struct {
	Action string `parser:"@('show'|'update'|'hold'|'release'|'create'|'delete')"`
}

func (b BaseAction) GetAction() string {
	return b.Action
}

type CommandID interface {
	GetID() string
}

type BaseID struct {
	ID string `parser:"( @String | @Ident | @Number )?"`
}

func (b BaseID) GetID() string {
	return b.ID
}
func (b BaseID) GetID() string {
	return b.ID
} 

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' ( @String | @Ident | @Number ) | ( @String | @Ident | @Number ) )"`
}
type EntityType struct {
	Node        bool `parser:"@'node'"`
	Partition   bool `parser:"| @'partition'"`
	Job         bool `parser:"| @'job'"`
	Reservation bool `parser:"| @'reservation'"`
}

type ShowCommand struct {
	BaseAction `parser:"@@"`
	Entity     *EntityType `parser:"@@"`
	BaseID     `parser:"@@"`
}
type UpdateCommand struct {
	BaseAction    `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}
type HoldCommand struct {
	BaseAction    `parser:"@@"`
	BaseID        `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}
type ReleaseCommand struct {
	BaseAction    `parser:"@@"`
	BaseID        `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}
type CreateCommand struct {
	BaseAction    `parser:"@@"`
	Entity        *EntityType `parser:"@@"`
	BaseID        `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}
type DeleteCommand struct {
	BaseAction `parser:"@@"`
	Entity     *EntityType `parser:"@@"`
	BaseID     `parser:"@@"`
}

type CControlCommand struct {
	Command any `parser:"@@"`
}

var CControlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `[-+]?("[^"]*"|'[^']*'|""|'')`},
	{Name: "Ident", Pattern: `[\+\-]?[a-zA-Z0-9][a-zA-Z0-9_\+\-\.,@:\[\]T]*`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Punct", Pattern: `[-=,:]`},
})

var CControlParser = participle.MustBuild[CControlCommand](
	participle.Lexer(CControlLexer),
	participle.Elide("whitespace"),
	participle.Union[any](
		ShowCommand{},
		UpdateCommand{},
		HoldCommand{},
		ReleaseCommand{},
		CreateCommand{},
		DeleteCommand{},
	),
)

func (c *CControlCommand) GetAction() string {
	if cmd, ok := c.Command.(CommandAction); ok {
		return cmd.GetAction()
	}
	return ""
}

func (e EntityType) String() string {
	switch {
	case e.Node:
		return "node"
	case e.Partition:
		return "partition"
	case e.Job:
		return "job"
	case e.Reservation:
		return "reservation"
	default:
		return ""
	}
}
func (c *CControlCommand) GetEntity() string {
	switch cmd := c.Command.(type) {
	case ShowCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case CreateCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case DeleteCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	}
	return ""
}

func (c *CControlCommand) GetID() string {
	if cmd, ok := c.Command.(CommandID); ok {
		return cmd.GetID()
	}
	return ""
}

func (c *CControlCommand) GetKVParamValue(key string) string {
	var params []*KeyValueParam
	switch cmd := c.Command.(type) {
	case UpdateCommand:
		params = cmd.KeyValueParam
	case HoldCommand:
		params = cmd.KeyValueParam
	case ReleaseCommand:
		params = cmd.KeyValueParam
	case CreateCommand:
		params = cmd.KeyValueParam
	}

	for _, param := range params {
		if strings.EqualFold(param.Key, key) {
			return param.Value
		}
	}
	return ""
}

func unquoteIfQuoted(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
func (c *CControlCommand) GetKVMaps() map[string]string {
	kvMap := make(map[string]string)
	switch cmd := c.Command.(type) {
	case UpdateCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case HoldCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case ReleaseCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case CreateCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	}
	return kvMap
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
		default:
			remainingArgs = append(remainingArgs, arg)
		}
	}
	return remainingArgs
}

func getCmdStringByArgs(commandArgs []string) string {
	var processedArgs []string
	for _, arg := range commandArgs {
		if arg == "" {
			processedArgs = append(processedArgs, "\"\"")
			continue
		}

		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			key := parts[0]
			value := parts[1]

			if value == "" {
				processedArgs = append(processedArgs, key+"=\"\"")
				continue
			}
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
				(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				processedArgs = append(processedArgs, arg)
			} else if strings.Contains(value, " ") {
				processedArgs = append(processedArgs, key+"="+strconv.Quote(value))
			} else {
				processedArgs = append(processedArgs, arg)
			}
		} else if strings.Contains(arg, " ") && !strings.HasPrefix(arg, "'") && !strings.HasPrefix(arg, "\"") {
			processedArgs = append(processedArgs, strconv.Quote(arg))
		} else {
			processedArgs = append(processedArgs, arg)
		}
	}
	cmdStr := strings.Join(processedArgs, " ")
	return cmdStr
}

func ParseCControlCommand(input string) (*CControlCommand, error) {
	return CControlParser.ParseString("", input)
}

func ParseCControlCommand(input string) (*CControlCommand, error) {
	return CControlParser.ParseString("", input)
}

