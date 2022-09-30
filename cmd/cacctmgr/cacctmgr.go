package main

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"
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
	if curLevel == protos.UserInfo_Admin {
		for key, value := range userMap {
			fmt.Printf("Account: %s\n", key)
			for _, userInfo := range value {
				fmt.Printf("UserName:%v Uid:%v AllowedPartition:%v AdminLevel:%v\n", userInfo.Name, userInfo.Uid, userInfo.AllowedPartition, userInfo.AdminLevel)
			}
			fmt.Println()
		}
	} else {
		fmt.Printf("Account: %s\n", curAccount)
		for _, userInfo := range userMap[curAccount] {
			fmt.Printf("UserName:%v Uid:%v AllowedPartition:%v AdminLevel:%v\n", userInfo.Name, userInfo.Uid, userInfo.AllowedPartition, userInfo.AdminLevel)
		}
	}
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

	if curLevel == protos.UserInfo_Admin {
		for _, account := range rootAccount {
			PraseAccountTree(account, accountMap)
			PrintAccountTree(account, accountMap)
		}
	} else {
		PraseAccountTree(curAccount, accountMap)
		PrintAccountTree(curAccount, accountMap)
	}
}

func PraseAccountTree(account string, accountMap map[string]*protos.AccountInfo) int {
	accountInfo := accountMap[account]

	sum := 0
	for _, child := range accountInfo.ChildAccount {
		n := PraseAccountTree(child, accountMap)
		sum = sum + n
	}

	if len(accountInfo.Users) != 0 {
		userString := fmt.Sprintf("|%v|", accountInfo.Users)
		sum += len(userString)
	}

	accountMap[account].Name = fmt.Sprintf(fmt.Sprintf("|%%-%ds|", sum-2), account)
	return len(accountMap[account].Name)
}

func PrintAccountTree(account string, accountMap map[string]*protos.AccountInfo) {
	if account == "" {
		return
	}
	curRoot := []string{account, ""}
	var printList []string
	line := ""
	frontPos := len(curRoot)
	l := len(accountMap[account].Name)
	for {
		front := curRoot[0]
		curRoot = curRoot[1:]
		frontPos--
		if front == "" {
			fmt.Println(line)
			line = ""
			if len(curRoot) == 0 {
				break
			}
			curRoot = append(curRoot, "")
			frontPos = len(curRoot)
		} else {
			info, ok := accountMap[front]
			if ok {
				printList = append(printList, front)
				line += info.Name
				if len(info.Users) > 0 {
					curRoot = append(curRoot, fmt.Sprintf("|%v|", info.Users))
				}
				for _, child := range info.ChildAccount {
					if len(accountMap[child].ChildAccount) == 0 && len(accountMap[child].Users) == 0 {
						curRoot = append(curRoot, child)
					} else {
						curRoot = append(curRoot, "")                  // expand
						copy(curRoot[frontPos+1:], curRoot[frontPos:]) // curRoot[i:] move back
						curRoot[frontPos] = child                      // insert into slice
					}
				}
			} else {
				line += front
			}
		}
	}
	var builder strings.Builder
	for i := 0; i < l; i++ {
		builder.WriteString("-")
	}
	fmt.Println(builder.String())
	for _, name := range printList {
		acc := accountMap[name]
		fmt.Printf("Name:%v Description:'%v' AllowedPartition:%v Qos:%v\n", name, acc.Description, acc.AllowedPartition, acc.Qos)
	}
	fmt.Println()
}

func Error(inf string, args ...interface{}) {
	out := fmt.Sprintf(inf, args...)
	fmt.Println(out)
	os.Exit(1)
}

func main() {
	if len(os.Args) <= 1 {
		Error("Arg must > 1")
	}

	confFile, err := ioutil.ReadFile("/etc/crane/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	confTxt := ServerAddr{}

	err = yaml.Unmarshal(confFile, &confTxt)
	if err != nil {
		log.Fatal(err)
	}
	ip := confTxt.ControlMachine
	port := confTxt.CraneCtldListenPort

	serverAddr := fmt.Sprintf("%s:%s", ip, port)
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)

	currentUser, err := user.Current()
	if err != nil {
		log.Fatal(err.Error())
	}

	find, curLevel, curAccount := QueryLevelAndAccount(currentUser.Name, stub)
	if find {
		//if curLevel == protos.UserInfo_None {
		//	fmt.Println("none")
		//} else if curLevel == protos.UserInfo_Operator {
		//	fmt.Println("operator")
		//} else if curLevel == protos.UserInfo_Admin {
		//	fmt.Println("admin")
		//}
	} else {
		fmt.Printf("%s, you are not a user in the system\n", currentUser.Name)
	}

	if os.Args[1] == "add" {
		if os.Args[2] == "account" {
			if curLevel != protos.UserInfo_Admin {
				Error("Permission error : You do not have permission to add account")
			}
			var req *protos.AddAccountRequest
			req = new(protos.AddAccountRequest)
			req.Account = new(protos.AccountInfo)
			os.Args = append(os.Args[:1], os.Args[3:]...) //delete first and second parameter
			var account = req.Account
			flag.StringVar(&account.Name, "name", "", "the name to identify account")
			flag.StringVar(&req.Account.Description, "describe", "", "some information to describe account")
			flag.StringVar(&req.Account.ParentAccount, "parent", "", "parent account")
			AllowedPartition := flag.String("partition", "", "the partition list which this account has access to")
			flag.StringVar(&req.Account.Qos, "Qos", "", "QOS of the account")
			flag.Parse()
			others := flag.Args()
			if len(others) > 0 {
				Error("Parameter error: If a parameter contains spaces, enclose the parameter with quotation marks\nisolated character list :%v", others)
			}

			stringSplit := strings.FieldsFunc(*AllowedPartition, func(r rune) bool { return strings.ContainsRune("[],", r) })
			for _, s := range stringSplit {
				account.AllowedPartition = append(account.AllowedPartition, strings.TrimSpace(s))
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
		} else if os.Args[2] == "user" {
			var req *protos.AddUserRequest
			req = new(protos.AddUserRequest)
			req.User = new(protos.UserInfo)
			os.Args = append(os.Args[:1], os.Args[3:]...) //delete first and second parameter

			var myUser = req.User
			flag.StringVar(&myUser.Name, "name", "", "the name to identify account")
			flag.StringVar(&myUser.Account, "account", "", "parent account")
			t := flag.String("level", "none", "user power level")
			AllowedPartition := flag.String("partition", "", "the partition list which this account has access to")
			flag.Parse()

			lu, err := user.Lookup(myUser.Name)
			if err != nil {
				log.Fatal(err)
			}
			i64, err := strconv.ParseInt(lu.Uid, 10, 64)
			if err == nil {
				myUser.Uid = uint32(i64)
			}

			stringSplit := strings.FieldsFunc(*AllowedPartition, func(r rune) bool { return strings.ContainsRune("[],", r) })
			for _, s := range stringSplit {
				myUser.AllowedPartition = append(myUser.AllowedPartition, strings.TrimSpace(s))
			}

			others := flag.Args()
			if len(others) > 0 {
				Error("Parameter error: If a parameter contains spaces, enclose the parameter with quotation marks\nisolated character list :%v", others)
			}

			if *t == "none" {
				myUser.AdminLevel = protos.UserInfo_None
			} else if *t == "operator" {
				myUser.AdminLevel = protos.UserInfo_Operator
			} else if *t == "admin" {
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
	} else if os.Args[1] == "delete" {
		var req *protos.DeleteEntityRequest

		if os.Args[2] == "account" {
			if curLevel != protos.UserInfo_Admin {
				Error("Permission error : You do not have permission to delete %s", os.Args[2])
			}
			req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_Account, Name: os.Args[3]}
		} else if os.Args[2] == "user" {
			ok, delLevel, delAccount := QueryLevelAndAccount(os.Args[3], stub)
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
			req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_User, Name: os.Args[3]}
		} else if os.Args[2] == "qos" {
			if curLevel == protos.UserInfo_Admin {
				Error("Permission error : You do not have permission to delete %s", os.Args[2])
			}
			req = &protos.DeleteEntityRequest{EntityType: protos.EntityType_Qos, Name: os.Args[3]}
		} else {
			Error("unknown entity name: {}", os.Args[2])
		}

		//fmt.Printf("Req:\n%v\n\n", req)
		reply, err := stub.DeleteEntity(context.Background(), req)
		if err != nil {
			panic("delete " + os.Args[2] + " " + os.Args[3] + " failed: " + err.Error())
		}
		if reply.GetOk() {
			fmt.Printf("delete %s %s success\n", os.Args[2], os.Args[3])
		} else {
			fmt.Printf("delete %s %s failed: %s\n", os.Args[2], os.Args[3], reply.GetReason())
		}
	} else if os.Args[1] == "set" {
		var req *protos.ModifyEntityRequest
		req = new(protos.ModifyEntityRequest)
		var t *string
		partition := flag.String("partition", "", "the partition list which this account has access to")
		if os.Args[2] == "account" {
			os.Args = append(os.Args[:1], os.Args[3:]...) //delete first and second parameter
			account := new(protos.AccountInfo)
			flag.StringVar(&account.Name, "name", "", "the name to identify account")
			flag.StringVar(&account.Description, "describe", "", "some information to describe account")
			flag.StringVar(&account.ParentAccount, "parent", "", "parent account")
			flag.StringVar(&account.Qos, "Qos", "", "QOS of the account")
			t = flag.String("type", "add", "the type to change allowed partition")
			flag.Parse()
			if curLevel == protos.UserInfo_Operator {
				if account.Name != curAccount {
					Error("Permission error : You do not have permission to modify other account")
				}
			} else if curLevel == protos.UserInfo_None {
				Error("Permission error : You do not have permission to modify account")
			}
			if account.ParentAccount == "" {
				isSet := false
				for _, arg := range os.Args {
					index := strings.Index(arg, "-parent")
					if index > 0 && index < 2 {
						isSet = true
					}
				}
				if !isSet {
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
			}
			stringSplit := strings.FieldsFunc(*partition, func(r rune) bool { return strings.ContainsRune("[],", r) })
			for _, s := range stringSplit {
				account.AllowedPartition = append(account.AllowedPartition, strings.TrimSpace(s))
			}
			req.NewEntity = &protos.ModifyEntityRequest_NewAccount{NewAccount: account}
		} else if os.Args[2] == "user" {
			os.Args = append(os.Args[:1], os.Args[3:]...) //delete first and second parameter
			myUser := new(protos.UserInfo)
			flag.StringVar(&myUser.Name, "name", "", "the name to identify account")
			flag.StringVar(&myUser.Account, "account", "", "parent account")
			le := flag.String("level", "none", "user power level")
			t = flag.String("type", "add", "the type to change allowed partition")
			flag.Parse()

			ok, setLevel, setAccount := QueryLevelAndAccount(myUser.Name, stub)
			if ok {
				if curLevel == protos.UserInfo_Operator {
					if setAccount != curAccount {
						Error("Permission error : You can't modify user in other account")
					} else if myUser.Account != "" {
						Error("Permission error : You can't modify user to other account")
					}
				} else if curLevel == protos.UserInfo_None {
					Error("Permission error : You do not have permission to modify user")
				}
				if curLevel.Number() <= setLevel.Number() {
					Error("Permission error : You can't modify user with the permission exceeds or equals to your permission")
				}
			}

			stringSplit := strings.FieldsFunc(*partition, func(r rune) bool { return strings.ContainsRune("[],", r) })
			for _, s := range stringSplit {
				myUser.AllowedPartition = append(myUser.AllowedPartition, strings.TrimSpace(s))
			}
			if *le == "" {
				ok, level, _ := QueryLevelAndAccount(myUser.Name, stub)
				if !ok {
					Error("user %s is not exist!", myUser.Name)
				}
				myUser.AdminLevel = level
			} else {
				if *le == "none" {
					myUser.AdminLevel = protos.UserInfo_None
				} else if *le == "operator" {
					myUser.AdminLevel = protos.UserInfo_Operator
				} else if *le == "admin" {
					myUser.AdminLevel = protos.UserInfo_Admin
				}
			}
			req.NewEntity = &protos.ModifyEntityRequest_NewUser{NewUser: myUser}
		} else {
			Error("Parameter error: please input set user or show account\n")
		}
		others := flag.Args()
		if len(others) > 0 {
			Error("Parameter error: If a parameter contains spaces, enclose the parameter with quotation marks\nisolated character list :%v", others)
		}

		if *t == "overwrite" {
			req.Type = protos.ModifyEntityRequest_Overwrite
		} else if *t == "add" {
			req.Type = protos.ModifyEntityRequest_Add
		} else if *t == "delete" {
			req.Type = protos.ModifyEntityRequest_Delete
		}
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
	} else if os.Args[1] == "show" {
		var req *protos.QueryEntityInfoRequest
		if os.Args[2] == "users" {
			req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User, Name: "All"}

			reply, err := stub.QueryEntityInfo(context.Background(), req)
			if err != nil {
				panic("query entity info failed: " + err.Error())
			}

			if reply.GetOk() {
				PrintAllUsers(reply.UserList, curLevel, curAccount)
			} else {
				fmt.Printf("Query all users failed! ")
			}
		} else if os.Args[2] == "accounts" {
			req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account, Name: "All"}
			reply, err := stub.QueryEntityInfo(context.Background(), req)
			if err != nil {
				panic("query entity info failed: " + err.Error())
			}

			if reply.GetOk() {
				PrintAllAccount(reply.AccountList, curLevel, curAccount)
			} else {
				fmt.Printf("Query all account failed! ")
			}
		} else {
			Error("Parameter error: please input show users or show accounts\n")
		}
	} else if os.Args[1] == "find" {
		if os.Args[2] == "user" {
			var req *protos.QueryEntityInfoRequest
			req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_User, Name: os.Args[3]}

			reply, err := stub.QueryEntityInfo(context.Background(), req)
			if err != nil {
				panic("query entity info failed: " + err.Error())
			}

			if reply.GetOk() {
				fmt.Printf("UserName:%v Uid:%v Account:%v AllowedPartition:%v AdminLevel:%v\n", reply.UserList[0].Name, reply.UserList[0].Uid, reply.UserList[0].Account, reply.UserList[0].AllowedPartition, reply.UserList[0].AdminLevel)
			} else {
				fmt.Printf("Query user %s failed! \n", os.Args[3])
			}
		} else if os.Args[2] == "account" {
			var req *protos.QueryEntityInfoRequest
			req = &protos.QueryEntityInfoRequest{EntityType: protos.EntityType_Account, Name: os.Args[3]}

			reply, err := stub.QueryEntityInfo(context.Background(), req)
			if err != nil {
				panic("query entity info failed: " + err.Error())
			}

			if reply.GetOk() {
				fmt.Printf("AccountName:%v Description:'%v' ParentAccount:%v ChildAccount:%v Users:%v AllowedPartition:%v QOS:%v\n", reply.AccountList[0].Name, reply.AccountList[0].Description, reply.AccountList[0].ParentAccount, reply.AccountList[0].ChildAccount, reply.AccountList[0].Users, reply.AccountList[0].AllowedPartition, reply.AccountList[0].Qos)
			} else {
				fmt.Printf("Query account %s failed! ", os.Args[3])
			}
		} else {
			Error("Parameter error: please input find user or show account\n")
		}
	} else {
		Error("Parameter error: please input add/delete/find/show/set\n")
	}
}
