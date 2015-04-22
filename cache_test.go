package orm

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

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

	CacheConsistent.Add("127.0.0.1:6379")
	SetDefaultCacheDb(10)
	SetDebug(true)

	_, err := NewDatabase("default", "mysql", "happy:passwd@tcp(127.0.0.1:3306)/mydatabase?charset=utf8&parseTime=true")
	if err != nil {
		t.Error(err)
	}
	b := new(userB)

	users := []userB{}
	b.Objects(b).Limit(1, 5).All(&users)
	if err != nil {
		t.Error(err)
	}
	for _, user := range users {
		t.Log(user.Uid, user.Alias, user.Lingshi)
		t.Log(user.hasRow, user.Cache)

	}

}
