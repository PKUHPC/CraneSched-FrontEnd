package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/xlab/treeprint"
	"log"
	"os"
	OSUser "os/user"
	"regexp"
	"strconv"
)

var (
	curLevel   protos.UserInfo_AdminLevel
	curAccount string
	stub       protos.CraneCtldClient
)

type ServerAddr struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`
}

func QueryLevelAndAccount(name string, stub protos.CraneCtldClient) (bool, protos.UserInfo_AdminLevel, string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("query entity info failed: " + err.Error())
	}

	if name == "root" {
		//root
		if reply.GetOk() {
			return true, protos.UserInfo_Admin, reply.UserList[0].Account
		} else {
			return true, protos.UserInfo_Admin, ""
		}
	}

	if reply.GetOk() {
		return true, reply.UserList[0].AdminLevel, reply.UserList[0].Account
	} else {
		return false, protos.UserInfo_None, ""
	}
}

func PrintAllUsers(userList []*protos.UserInfo, curLevel protos.UserInfo_AdminLevel, curAccount string) {
	if len(userList) == 0 {
		fmt.Println("There is no user in crane")
		return
	}
	//slice to map
	userMap := make(map[string][]*protos.UserInfo)
	for _, userInfo := range userList {
		if list, ok := userMap[userInfo.Account]; ok {
			userMap[userInfo.Account] = append(list, userInfo)
		} else {
			var list = []*protos.UserInfo{userInfo}
			userMap[userInfo.Account] = list
		}
	}

	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "AdminLevel"})
	table.SetAutoFormatHeaders(false)
	tableData := make([][]string, len(userMap))
	if curLevel == protos.UserInfo_Admin {
		for key, value := range userMap {
			tableData = append(tableData, []string{key})
			for _, userInfo := range value {
				if len(userInfo.AllowedPartitionQosList) == 0 {
					tableData = append(tableData, []string{
						key,
						userInfo.Name,
						strconv.FormatUint(uint64(userInfo.Uid), 10),
						"",
						"",
						"",
						fmt.Sprintf("%v", userInfo.AdminLevel)})
				}
				for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
					tableData = append(tableData, []string{
						key,
						userInfo.Name,
						strconv.FormatUint(uint64(userInfo.Uid), 10),
						allowedPartitionQos.PartitionName,
						fmt.Sprintf("%v", allowedPartitionQos.QosList),
						allowedPartitionQos.DefaultQos,
						fmt.Sprintf("%v", userInfo.AdminLevel)})
				}
			}
		}
	} else {
		tableData = append(tableData, []string{curAccount})
		for _, userInfo := range userMap[curAccount] {
			for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
				tableData = append(tableData, []string{
					curAccount,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					allowedPartitionQos.PartitionName,
					fmt.Sprintf("%v", allowedPartitionQos.QosList),
					allowedPartitionQos.DefaultQos,
					fmt.Sprintf("%v", userInfo.AdminLevel)})
			}
		}
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAllQos(qosList []*protos.QosInfo) {
	if len(qosList) == 0 {
		fmt.Println("There is no qos in crane")
		return
	}

	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Name", "Description", "Priority", "MaxJobsPerUser", "MaxCpusPerUser", "MaxTimeLimitPerTask"})
	table.SetAutoFormatHeaders(false)
	tableData := make([][]string, len(qosList))
	for _, info := range qosList {
		tableData = append(tableData, []string{
			info.Name,
			info.Description,
			fmt.Sprintf("%d", info.Priority),
			fmt.Sprintf("%d", info.MaxJobsPerUser),
			fmt.Sprintf("%d", info.MaxCpusPerUser),
			fmt.Sprintf("%d", info.MaxTimeLimitPerTask)})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAllAccount(accountList []*protos.AccountInfo, curLevel protos.UserInfo_AdminLevel, curAccount string) {
	if len(accountList) == 0 {
		fmt.Println("There is no account in crane")
		return
	}
	//slice to map and find the root account
	accountMap := make(map[string]*protos.AccountInfo)
	rootAccount := make([]string, 0)
	for _, accountInfo := range accountList {
		accountMap[accountInfo.Name] = accountInfo
		if accountInfo.ParentAccount == "" {
			rootAccount = append(rootAccount, accountInfo.Name)
		}
	}

	//print account tree
	var tree treeprint.Tree
	if curLevel == protos.UserInfo_Admin {
		tree = treeprint.NewWithRoot("AccountTree")
		for _, account := range rootAccount {
			PraseAccountTree(tree, account, accountMap)
		}
	} else {
		tree = treeprint.New()
		PraseAccountTree(tree, curAccount, accountMap)
	}
	fmt.Println(tree.String())

	//print account table
	PrintAccountTable(accountList)
}

func PrintAccountTable(accountList []*protos.AccountInfo) {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Name", "Description", "AllowedPartition", "DefaultQos", "AllowedQosList"})
	table.SetAutoFormatHeaders(false)

	tableData := make([][]string, len(accountList))
	for _, accountInfo := range accountList {
		tableData = append(tableData, []string{
			accountInfo.Name,
			accountInfo.Description,
			fmt.Sprintf("%v", accountInfo.AllowedPartitions),
			accountInfo.DefaultQos,
			fmt.Sprintf("%v", accountInfo.AllowedQosList)})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func Error(inf string, args ...interface{}) {
	out := fmt.Sprintf(inf, args...)
	fmt.Println(out)
	os.Exit(1)
}

func PraseAccountTree(parentTreeRoot treeprint.Tree, account string, accountMap map[string]*protos.AccountInfo) {

	if account == "" {
		return
	}
	if len(accountMap[account].ChildAccounts) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccounts {
			PraseAccountTree(branch, child, accountMap)
		}
	}
}

func AddAccount(account *protos.AccountInfo) {
	if curLevel != protos.UserInfo_Admin {
		Error("Permission error : You do not have permission to add account")
	}
	if account.Name == "=" {
		Error("Parameter error : Account name empty")
	}
	var req *protos.AddAccountRequest
	req = new(protos.AddAccountRequest)
	req.Account = account
	if account.DefaultQos == "" && len(account.AllowedQosList) > 0 {
		account.DefaultQos = account.AllowedQosList[0]
	}
	if account.DefaultQos != "" {
		find := false
		for _, qos := range account.AllowedQosList {
			if qos == account.DefaultQos {
				find = true
				break
			}
		}
		if !find {
			Error("Parameter error : default qos %s not contain in allowed qos list", account.DefaultQos)
		}
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		panic("add account failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Add account success!")
	} else {
		fmt.Printf("Add account failed: %s\n", reply.GetReason())
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string) {
	lu, err := OSUser.Lookup(user.Name)
	if err != nil {
		log.Fatal(err)
	}
	var req *protos.AddUserRequest
	req = new(protos.AddUserRequest)
	req.User = user
	for _, par := range partition {
		user.AllowedPartitionQosList = append(user.AllowedPartitionQosList, &protos.UserInfo_AllowedPartitionQos{PartitionName: par})
	}

	i64, err := strconv.ParseInt(lu.Uid, 10, 64)
	if err == nil {
		user.Uid = uint32(i64)
	}

	if level == "none" {
		user.AdminLevel = protos.UserInfo_None
	} else if level == "operator" {
		user.AdminLevel = protos.UserInfo_Operator
	} else if level == "admin" {
		user.AdminLevel = protos.UserInfo_Admin
	}

	if curLevel == protos.UserInfo_None {
		Error("Permission error : You do not have permission to add user")
	} else if curLevel == protos.UserInfo_Operator {
		if user.Account != curAccount {
			Error("Permission error : You can't add user to other account")
		}
		if user.AdminLevel != protos.UserInfo_None {
			Error("Permission error : You cannot add users with permissions")
		}
	}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		panic("add user failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Add user success!")
	} else {
		fmt.Printf("Add user failed: %s\n", reply.GetReason())
	}
}

func AddQos(qos *protos.QosInfo) {
	if curLevel != protos.UserInfo_Admin {
		Error("Permission error : You do not have permission to add qos")
	}
	var req *protos.AddQosRequest
	req = new(protos.AddQosRequest)
	req.Qos = qos

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		panic("add qos failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Add qos success!")
	} else {
		fmt.Printf("Add qos failed: %s\n", reply.GetReason())
	}
}

func DeleteAccount(name string) {
	var req *protos.DeleteEntityRequest

	if curLevel != protos.UserInfo_Admin {
		Error("Permission error : You do not have permission to delete account")
	}
	req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_Account, Name: name}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		panic("Delete account " + name + " failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("Delete account %s success\n", name)
	} else {
		fmt.Printf("Delete account %s failed: %s\n", name, reply.GetReason())
	}
}

func DeleteUser(name string) {
	var req *protos.DeleteEntityRequest

	ok, delLevel, delAccount := QueryLevelAndAccount(name, stub)
	if ok {
		if curLevel == protos.UserInfo_Operator {
			if delAccount != curAccount {
				Error("Permission error : You can't delete user in other account")
			}
		}
		if curLevel.Number() <= delLevel.Number() {
			Error("Permission error : You can't delete user with the permission exceeds or equals to your permission")
		}
	}
	req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_User, Name: name}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		panic("delete User " + name + " failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("delete User %s success\n", name)
	} else {
		fmt.Printf("delete User %s failed: %s\n", name, reply.GetReason())
	}
}

func DeleteQos(name string) {
	var req *protos.DeleteEntityRequest

	if curLevel != protos.UserInfo_Admin {
		Error("Permission error : You do not have permission to delete Qos")
	}
	req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_Qos, Name: name}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		panic("delete Qos " + name + " failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("delete Qos %s success\n", name)
	} else {
		fmt.Printf("delete Qos %s failed: %s\n", name, reply.GetReason())
	}
}

func ModifyAccount(modifyItem string, name string, requestType protos.ModifyEntityRequest_OperatorType) {
	itemLeft, itemRight := ParseEquation(modifyItem)
	if !checkAccountFieldName(itemLeft) {
		Error("Field name %s not exist!", itemLeft)
	}
	if curLevel == protos.UserInfo_None {
		Error("Permission error : You do not have permission to modify account")
	}

	req := protos.ModifyEntityRequest{
		Lhs:        itemLeft,
		Rhs:        itemRight,
		Name:       name,
		Type:       requestType,
		EntityType: protos.EntityType_Account,
	}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		panic("Modify information failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Modify information success!")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ModifyUser(modifyItem string, name string, partition string, requestType protos.ModifyEntityRequest_OperatorType) {
	itemLeft, itemRight := ParseEquation(modifyItem)
	if !checkUserFieldName(itemLeft) {
		Error("Field name %s not exist!", itemLeft)
	}
	if itemLeft == "admin_level" {
		if itemRight != "none" && itemRight != "operator" && itemRight != "admin" {
			Error("Unknown admin_level, please enter one of {none, operator, admin}")
		}
	}

	if curLevel == protos.UserInfo_None {
		Error("Permission error : You do not have permission to modify user")
	}

	req := protos.ModifyEntityRequest{
		Lhs:        itemLeft,
		Rhs:        itemRight,
		Name:       name,
		Partition:  partition,
		Type:       requestType,
		EntityType: protos.EntityType_User,
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		panic("Modify information failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Modify information success")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ModifyQos(modifyItem string, name string) {
	itemLeft, itemRight := ParseEquation(modifyItem)
	if !checkQosFieldName(itemLeft) {
		Error("Field name %s not exist!", itemLeft)
	}

	req := protos.ModifyEntityRequest{
		Lhs:        itemLeft,
		Rhs:        itemRight,
		Name:       name,
		Type:       protos.ModifyEntityRequest_Overwrite,
		EntityType: protos.EntityType_Qos,
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		panic("Modify information failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Println("Modify information success!")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ShowAccounts() {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account}
	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("Query account info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAllAccount(reply.AccountList, curLevel, curAccount)
	} else {
		fmt.Println("Can't find any account!")
	}
}

func ShowUser(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("Query user info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAllUsers(reply.UserList, curLevel, curAccount)
	} else {
		if name == "" {
			fmt.Println("Can't find any user!")
		} else {
			fmt.Printf("Can't find user %s\n", name)
		}
	}
}

func ShowQos(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Qos, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("Query qos info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAllQos(reply.QosList)
	} else {
		if name == "" {
			fmt.Println("Can't find any qos!")
		} else {
			fmt.Printf("Can't find qos %s\n", name)
		}
	}
}

func FindAccount(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("query entity info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAccountTable(reply.AccountList)
	} else {
		fmt.Printf("Can't find account %s\n", name)
	}
}

func ParseEquation(s string) (left string, right string) {
	reg := regexp.MustCompile("^([\\w]+)=(.*)$")
	match := reg.FindAllStringSubmatch(s, -1)
	if match == nil || len(match[0]) != 3 {
		Error("Parse equation '%s' fail,it may not match regex '^([\\w]+)=([\\w]+)$'", s)
	}
	return match[0][1], match[0][2]
}

func checkUserFieldName(s string) bool {
	switch s {
	case "name", "account", "default_qos", "allowed_qos_list", "allowed_partition", "admin_level":
		return true
	}
	return false
}

func checkAccountFieldName(s string) bool {
	switch s {
	case "name", "description", "parent_account", "allowed_partition", "default_qos", "allowed_qos_list":
		return true
	}
	return false
}

func checkQosFieldName(s string) bool {
	switch s {
	case "name", "description", "priority", "max_jobs_per_user", "max_cpus_per_user", "max_time_limit_per_task":
		return true
	}
	return false
}

func Preparation() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	currentUser, err := OSUser.Current()
	if err != nil {
		log.Fatal(err.Error())
	}

	_, curLevel, curAccount = QueryLevelAndAccount(currentUser.Name, stub)
}
