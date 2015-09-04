package orm

import (
	"fmt"
	"sync"
	"testing"
	//"time"

	_ "github.com/go-sql-driver/mysql"
)

func init() {

	_, err := NewDatabase("default", "mysql", "happy:passwd@tcp(127.0.0.1:3306)/mydatabase?charset=utf8&parseTime=true")
	if err != nil {
		panic(err)
	}
	SetDebug(true)
	AddCacheAddress("127.0.0.1:6379", "")
	SetDefaultCacheDb(0)

}

type userB struct {
	CacheHook
	Uid     int64  `field:"Id" index:"pk"  cache:"user" `
	Alias   string `field:"Alias"`
	Lingshi int64  `field:"Lingshi"`
	//LogoutTime time.Time `field:"updated_at"`
}
type userA struct {
	DBHook
	Uid     int64  `field:"Id" index:"pk"  cache:"user" `
	Alias   string `field:"Alias"`
	Lingshi int64  `field:"Lingshi"`
	//LogoutTime time.Time `field:"updated_at"`
}

func (self *userB) GetTableName() string {
	return "user_disk"
}
func (self *userA) GetTableName() string {
	return "user_disk"
}

func Test_connect(t *testing.T) {
	OpenSyncUpdate = true
	OpenSyncDelete = true
	b := new(userB)

	users := []*userB{}
	b.Objects(b).Limit(1, 10).All(&users)

	for _, user := range users {
		t.Log(user)
		user.Incrby("Lingshi", 1)
		user.Save()

		//t.Log(user.Uid, user.Lingshi)
		//user.Incrby("Lingshi", 1)
		//t.Log(user.Uid, user.Lingshi)
		//go user.Save()
		user.Delete()
	}
	for i := 0; i < 10; i++ {
		sql := <-SqlSyncHook
		t.Log(sql)
	}
}

func Test_alias(t *testing.T) {
	OpenSyncUpdate = false
	OpenSyncDelete = false
	b := new(userA)
	rows, _ := b.Objects(b).Query()
	for rows.Next() {
		nb := new(userA)
		err := rows.Scan(nb)
		t.Log(err, nb.Uid, nb.Lingshi)
	}
}

func Test_Delete(t *testing.T) {
	b := new(userB)
	b.Uid = 10000
	b.Objects(b).One()
	//b.Incrby("Lingshi", 1)
	b.Delete()
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
