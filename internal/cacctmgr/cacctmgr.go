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
	table.SetHeader([]string{"Account", "UserName", "Uid", "AllowedPartition", "AdminLevel"})
	table.SetAutoFormatHeaders(false)
	tableData := make([][]string, len(userMap))
	if curLevel == protos.UserInfo_Admin {
		for key, value := range userMap {
			tableData = append(tableData, []string{key})
			for _, userInfo := range value {
				tableData = append(tableData, []string{
					key,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					fmt.Sprintf("%v", userInfo.AllowedPartition),
					fmt.Sprintf("%v", userInfo.AdminLevel)})
			}
		}
	} else {
		tableData = append(tableData, []string{curAccount})
		for _, userInfo := range userMap[curAccount] {
			tableData = append(tableData, []string{
				curAccount,
				userInfo.Name,
				strconv.FormatUint(uint64(userInfo.Uid), 10),
				fmt.Sprintf("%v", userInfo.AllowedPartition),
				fmt.Sprintf("%v", userInfo.AdminLevel)})
		}
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAllAccount(accountList []*protos.AccountInfo, curLevel protos.UserInfo_AdminLevel, curAccount string) {
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
	table.SetHeader([]string{"Name", "Description", "AllowPartition", "Qos"})
	table.SetAutoFormatHeaders(false)

	tableData := make([][]string, len(accountMap))
	for name, info := range accountMap {
		tableData = append(tableData, []string{
			name,
			info.Description,
			fmt.Sprintf("%v", info.AllowedPartition),
			info.Qos})
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

func AddAccount(name string, describe string, parent string, partition []string, Qos string) {
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
	account.Qos = Qos
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
	myUser.AllowedPartition = partition

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
		panic("delete account " + name + " failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("delete account %s success\n", name)
	} else {
		fmt.Printf("delete account %s failed: %s\n", name, reply.GetReason())
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

func SetAccount(name string, describe string, parentPtr *string, Qos string) {
	var req *protos.ModifyEntityRequest
	req = new(protos.ModifyEntityRequest)
	account := new(protos.AccountInfo)
	account.Name = name
	account.Description = describe
	account.Qos = Qos

	if curLevel == protos.UserInfo_Operator {
		if account.Name != curAccount {
			Error("Permission error : You do not have permission to modify other account")
		}
	} else if curLevel == protos.UserInfo_None {
		Error("Permission error : You do not have permission to modify account")
	}

	//if operator don't set parent account, use original value
	if parentPtr == nil {
		var req *protos.QueryEntityInfoRequest
		req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account, Name: account.Name}
		reply, err := stub.QueryEntityInfo(context.Background(), req)
		if err != nil {
			panic("query entity info failed: " + err.Error())
		}
		if reply.GetOk() {
			account.ParentAccount = reply.AccountList[0].ParentAccount
		} else {
			Error("Modify information failed: the account %s does not exist\n", account.Name)
		}
	}

	req.NewEntity = &protos.ModifyEntityRequest_NewAccount{NewAccount: account}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), req)
	if err != nil {
		panic("Modify information failed: " + err.Error())
	}
	if reply.GetOk() {
		fmt.Printf("Modify information success\n")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func SetUser(name string, account string, levelPtr *string) {
	var req *protos.ModifyEntityRequest
	req = new(protos.ModifyEntityRequest)

	setUser := new(protos.UserInfo)
	setUser.Name = name
	setUser.Account = account

	ok, setLevel, setAccount := QueryLevelAndAccount(setUser.Name, stub)
	if ok {
		if curLevel == protos.UserInfo_Operator {
			if setAccount != curAccount {
				Error("Permission error : You can't modify user in other account")
			} else if setUser.Account != "" {
				Error("Permission error : You can't modify user to other account")
			}
		} else if curLevel == protos.UserInfo_None {
			Error("Permission error : You do not have permission to modify user")
		}
		if curLevel.Number() <= setLevel.Number() {
			Error("Permission error : You can't modify user with the permission exceeds or equals to your permission")
		}
	} else {
		Error("user %s is not exist!", setUser.Name)
	}

	if levelPtr == nil {
		setUser.AdminLevel = setLevel
	} else {
		if *levelPtr == "none" {
			setUser.AdminLevel = protos.UserInfo_None
		} else if *levelPtr == "operator" {
			setUser.AdminLevel = protos.UserInfo_Operator
		} else if *levelPtr == "admin" {
			setUser.AdminLevel = protos.UserInfo_Admin
		}
	}
	req.NewEntity = &protos.ModifyEntityRequest_NewUser{NewUser: setUser}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), req)
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
		fmt.Printf("Query all users failed! ")
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
		fmt.Printf("AccountName:%v Description:'%v' ParentAccount:%v ChildAccount:%v Users:%v AllowedPartition:%v QOS:%v\n", reply.AccountList[0].Name, reply.AccountList[0].Description, reply.AccountList[0].ParentAccount, reply.AccountList[0].ChildAccount, reply.AccountList[0].Users, reply.AccountList[0].AllowedPartition, reply.AccountList[0].Qos)
	} else {
		fmt.Printf("Query account %s failed! ", name)
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
		fmt.Printf("UserName:%v Uid:%v Account:%v AllowedPartition:%v AdminLevel:%v\n", reply.UserList[0].Name, reply.UserList[0].Uid, reply.UserList[0].Account, reply.UserList[0].AllowedPartition, reply.UserList[0].AdminLevel)
	} else {
		fmt.Printf("Query user %s failed! \n", name)
	}
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
