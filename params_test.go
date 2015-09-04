package orm

import "testing"
import "time"
import "fmt"

func Test_execSelect(t *testing.T) {
	OpenSyncUpdate = true

	params := Params{}
	params.SetField("a", "b", "c")
	params.SetTable("eeesfsfe")

	params.Filter("field__gt", 1)
	params.Filter("bbb__lt", 2)
	params.Change("a", 1)
	params.Change("b__sub", 1)
	params.Change("c__div", 1)
	//t.Log(params.execSelect())
	str, _ := driversql["mysql"](params).Select()
	t.Log(str)
	str, _ = driversql["mysql"](params).Delete()
	t.Log(str)
	str, _ = driversql["mysql"](params).Insert()
	t.Log(str)
	go func() {
		sql := <-SqlSyncHook
		fmt.Println("SYNC ", sql)
	}()
	str, _ = driversql["mysql"](params).Update()
	t.Log(str)
	str, _ = driversql["mysql"](params).Count()
	t.Log(str)
	time.Sleep(2)
}
