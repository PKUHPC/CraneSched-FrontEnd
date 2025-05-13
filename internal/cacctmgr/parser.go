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
 
 // CAcctMgrCommand represents the parsed command line for cacctmgr
 type CAcctMgrCommand struct {
	 Action      *ActionType      `parser:"@@"`
	 Resource    *ResourceType    `parser:"@@?"`
	 KVParams    []*KeyValueParam `parser:"@@*"`
	 GlobalFlags []*Flag          `parser:"@@*"`
 }
 
 // ActionType represents the action to perform
 type ActionType struct {
	 Add     bool `parser:"@'add'"`
	 Delete  bool `parser:"| @'delete'"`
	 Block   bool `parser:"| @'block'"`
	 Unblock bool `parser:"| @'unblock'"`
	 Modify  bool `parser:"| @'modify'"`
	 Show    bool `parser:"| @'show'"`
	 Find    bool `parser:"| @'find'"`
	 Help    bool `parser:"| @'help'"`
 }
 
 // ResourceType represents the resource to operate on
 type ResourceType struct {
	 Account bool `parser:"@'account'"`
	 User    bool `parser:"| @'user'"`
	 Qos     bool `parser:"| @'qos'"`
 }
 
 // Flag represents a command line flag
 type Flag struct {
	 Name  string `parser:"'-' '-'? @Ident"`
	 Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
 }
 
 type KeyValueParam struct {
	 Key   string `parser:"@Ident"`
	 Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
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
 )
 
 // ParseCAcctMgrCommand parses a command line string into a CAcctMgrCommand struct
 func ParseCAcctMgrCommand(input string) (*CAcctMgrCommand, error) {
	 return CAcctMgrParser.ParseString("", input)
 }
 
 // String returns the string representation of an ActionType
 func (a ActionType) String() string {
	 switch {
	 case a.Add:
		 return "add"
	 case a.Delete:
		 return "delete"
	 case a.Block:
		 return "block"
	 case a.Unblock:
		 return "unblock"
	 case a.Modify:
		 return "modify"
	 case a.Show:
		 return "show"
	 case a.Find:
		 return "find"
	 case a.Help:
		 return "help"
	 default:
		 return ""
	 }
 }
 
 // String returns the string representation of a ResourceType
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
 
 // GetAction
 func (c *CAcctMgrCommand) GetAction() string {
	 if c.Action == nil {
		 return ""
	 }
	 return c.Action.String()
 }
 
 // GetResource
 func (c *CAcctMgrCommand) GetResource() string {
	 if c.Resource == nil {
		 return ""
	 }
	 return c.Resource.String()
 }
 
 // GetKVParamValue
 func (c *CAcctMgrCommand) GetKVParamValue(key string) string {
	 for _, param := range c.KVParams {
		 if strings.EqualFold(param.Key, strings.ToLower(key)) {
			 return param.Value
		 }
	 }
	 return ""
 }
 
 // GetKVMaps
 func (c *CAcctMgrCommand) GetKVMaps() map[string]string {
	 kvMap := make(map[string]string)
	 for _, param := range c.KVParams {
		 kvMap[param.Key] = param.Value
	 }
	 return kvMap
 }
 
 func (c *CAcctMgrCommand) GetGlobalFlag(name string) (string, bool) {
	 for _, flag := range c.GlobalFlags {
		 if strings.EqualFold(flag.Name, name) {
			 return flag.Value, true
		 }
	 }
	 return "", false
 }
 
 func (c *CAcctMgrCommand) String() string {
	 var parts []string
 
	 if c.Action != nil {
		 parts = append(parts, c.Action.String())
	 }
 
	 if c.Resource != nil {
		 parts = append(parts, c.Resource.String())
	 }
 
	 for _, flag := range c.GlobalFlags {
		 if flag.Value != "" {
			 parts = append(parts, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
		 } else {
			 parts = append(parts, fmt.Sprintf("--%s", flag.Name))
		 }
	 }
 
	 for _, kv := range c.KVParams {
		 parts = append(parts, fmt.Sprintf("%s=%s", kv.Key, kv.Value))
	 }
 
	 return strings.Join(parts, " ")
 }
 
 // processGlobalFlags
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
 