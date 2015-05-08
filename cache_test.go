package orm

import (
	"fmt"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func init() {

	_, err := NewDatabase("default", "mysql", "happy:Cyup3EdezxW@tcp(192.168.0.50:3306)/xiyou_default?charset=utf8&parseTime=true") //"happy:passwd@tcp(127.0.0.1:3306)/mydatabase?charset=utf8&parseTime=true")
	if err != nil {
		panic(err)
	}
	SetDebug(true)
	UseHashCache(false)
	SetCacheAddress([]string{"127.0.0.1:6379"})
	SetDefaultCacheDb(0)
}

type userB struct {
	CacheModule
	Uid     int64  `field:"Id" index:"pk"  cache:"user" `
	Alias   string `field:"Alias"`
	Lingshi int64  `field:"Lingshi"	`
}

func (self *userB) GetTableName() string {
	return "user_disk"
}

func Test_connect(t *testing.T) {

	b := new(userB)

	users := []userB{}
	b.Objects(b).Limit(1, 10).All(&users)

	for _, user := range users {
		t.Log(user)
		user.Incrby("Lingshi", 1)
		go user.Save()

		//t.Log(user.Uid, user.Lingshi)
		//user.Incrby("Lingshi", 1)
		//t.Log(user.Uid, user.Lingshi)
		//go user.Save()
	}

}

func Test_OneObj(t *testing.T) {
	b := new(userB)
	b.Uid = 10000
	b.Objects(b).OneOnCache()
	b.Incrby("Lingshi", 1)
	go b.Save()
}

type User struct {
	sync.RWMutex
	Uid int
	N   int
}

func (self *User) Set(n int) {
	self.Lock()
	defer self.Unlock()
	self.N += n
}

func (self *User) Save(uid, n int) {
	self.Lock()
	defer self.Unlock()
	fmt.Println(self.Uid, uid, self.N, n)

}
func Test_list(t *testing.T) {

	testList1 := []User{}
	testList1 = append(testList1, User{Uid: 1, N: 100}, User{Uid: 2, N: 200}, User{Uid: 3, N: 300}, User{Uid: 4, N: 400})

	for _, user := range testList1 {
		user.Set(1)
		go user.Save(user.Uid, user.N)
	}

	testList2 := []*User{}
	testList2 = append(testList2, &User{Uid: 1, N: 100}, &User{Uid: 2, N: 200}, &User{Uid: 3, N: 300}, &User{Uid: 4, N: 400})

	for _, user := range testList2 {
		user.Set(1)
		go user.Save(user.Uid, user.N)
	}

}
