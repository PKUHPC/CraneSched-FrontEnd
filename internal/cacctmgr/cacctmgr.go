package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/xlab/treeprint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"os/user"
	"regexp"
	"strconv"
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
						allowedPartitionQos.Name,
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
					allowedPartitionQos.Name,
					fmt.Sprintf("%v", allowedPartitionQos.QosList),
					allowedPartitionQos.DefaultQos,
					fmt.Sprintf("%v", userInfo.AdminLevel)})
			}
		}
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
	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Name", "Description", "AllowPartition", "DefaultQos", "AllowedQosList"})
	table.SetAutoFormatHeaders(false)

	tableData := make([][]string, len(accountMap))
	for name, info := range accountMap {
		tableData = append(tableData, []string{
			name,
			info.Description,
			fmt.Sprintf("%v", info.AllowedPartition),
			info.DefaultQos,
			fmt.Sprintf("%v", info.AllowedQos)})
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

	if len(accountMap[account].ChildAccount) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccount {
			PraseAccountTree(branch, child, accountMap)
		}
	}
}

var (
	curLevel   protos.UserInfo_AdminLevel
	curAccount string
	stub       protos.CraneCtldClient
)

func AddAccount(name string, describe string, parent string, partition []string, defaultQos string, qosList []string) {
	if curLevel != protos.UserInfo_Admin {
		Error("Permission error : You do not have permission to add account")
	}

	var req *protos.AddAccountRequest
	req = new(protos.AddAccountRequest)
	req.Account = new(protos.AccountInfo)
	var account = req.Account
	account.Name = name
	account.Description = describe
	account.ParentAccount = parent
	account.AllowedPartition = partition
	account.AllowedQos = qosList
	if defaultQos == "" && len(qosList) > 0 {
		account.DefaultQos = qosList[0]
	} else {
		account.DefaultQos = defaultQos
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		panic("add account failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("add account success\n")
	} else {
		fmt.Printf("add account failed: %s\n", reply.GetReason())
	}
}

func AddUser(name string, account string, partition []string, level string) {
	var req *protos.AddUserRequest
	req = new(protos.AddUserRequest)
	req.User = new(protos.UserInfo)
	var myUser = req.User
	myUser.Name = name
	myUser.Account = account
	for _, par := range partition {
		myUser.AllowedPartitionQosList = append(myUser.AllowedPartitionQosList, &protos.UserInfoAllowedPartitionQos{Name: par})
	}

	lu, err := user.Lookup(myUser.Name)
	if err != nil {
		log.Fatal(err)
	}
	i64, err := strconv.ParseInt(lu.Uid, 10, 64)
	if err == nil {
		myUser.Uid = uint32(i64)
	}

	if level == "none" {
		myUser.AdminLevel = protos.UserInfo_None
	} else if level == "operator" {
		myUser.AdminLevel = protos.UserInfo_Operator
	} else if level == "admin" {
		myUser.AdminLevel = protos.UserInfo_Admin
	}

	if curLevel == protos.UserInfo_None {
		Error("Permission error : You do not have permission to add user")
	} else if curLevel == protos.UserInfo_Operator {
		if myUser.Account != curAccount {
			Error("Permission error : You can't add user to other account")
		}
		if myUser.AdminLevel != protos.UserInfo_None {
			Error("Permission error : You cannot add users with permissions")
		}
	}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		panic("add user failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("add user success\n")
	} else {
		fmt.Printf("add user failed: %s\n", reply.GetReason())
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

	if curLevel == protos.UserInfo_Admin {
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
		ItemLeft:   itemLeft,
		ItemRight:  itemRight,
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
		fmt.Printf("Modify information success\n")
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
		ItemLeft:   itemLeft,
		ItemRight:  itemRight,
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
		fmt.Printf("Modify information success\n")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ShowAccounts() {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account}
	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("query entity info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAllAccount(reply.AccountList, curLevel, curAccount)
	} else {
		fmt.Printf("Query all account failed! ")
	}
}

func ShowUsers() {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("query entity info failed: " + err.Error())
	}

	if reply.GetOk() {
		PrintAllUsers(reply.UserList, curLevel, curAccount)
	} else {
		fmt.Println("Query all users failed! ")
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
		fmt.Printf("AccountName:%v Description:'%v' ParentAccount:%v ChildAccount:%v Users:%v AllowedPartition:%v QOS:%v\n", reply.AccountList[0].Name, reply.AccountList[0].Description, reply.AccountList[0].ParentAccount, reply.AccountList[0].ChildAccount, reply.AccountList[0].Users, reply.AccountList[0].AllowedPartition, reply.AccountList[0].DefaultQos)
	} else {
		fmt.Printf("Query account %s failed! \n", name)
	}
}

func FindUser(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		panic("query entity info failed: " + err.Error())
	}

	if reply.GetOk() {
		fmt.Printf("UserName:%v Uid:%v Account:%v AllowedPartition:%v AdminLevel:%v\n", reply.UserList[0].Name, reply.UserList[0].Uid, reply.UserList[0].Account, "", reply.UserList[0].AdminLevel)
	} else {
		fmt.Printf("Query user %s failed! \n", name)
	}
}

func ParseEquation(s string) (left string, right string) {
	reg := regexp.MustCompile("^([\\w]+)=([\\w]+)$")
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

func Init() {

	config := util.ParseConfig()
	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub = protos.NewCraneCtldClient(conn)

	currentUser, err := user.Current()
	if err != nil {
		log.Fatal(err.Error())
	}

	_, curLevel, curAccount = QueryLevelAndAccount(currentUser.Name, stub)
	//if find {
	//if curLevel == protos.UserInfo_None {
	//	fmt.Println("none")
	//} else if curLevel == protos.UserInfo_Operator {
	//	fmt.Println("operator")
	//} else if curLevel == protos.UserInfo_Admin {
	//	fmt.Println("admin")
	//}
	//} else {
	//	fmt.Printf("%s, you are not a user in the system\n", currentUser.Name)
	//}
}
