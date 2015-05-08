package orm

import (
	"database/sql"
	"fmt"
	"strings"
)

var NULL_LIMIT = [2]int{0, 0}
var databases = map[string]*Database{}

//读写数据库名称
var readDbConnectName, writeDbConnectName string = "default", "default"

func SetReadConnectName(name string) {
	readDbConnectName = name
}
func SetWriteConnectName(name string) {
	writeDbConnectName = name
}

type Database struct {
	*sql.DB
	Name           string
	DriverName     string
	DataSourceName string
}

func (self *Database) Conn() (err error) {
	self.DB, err = sql.Open(self.DriverName, self.DataSourceName)

	return
}

func NewDatabase(name, driverName, dataSourceName string) (database *Database, err error) {
	if database, ok := databases[name]; !ok {
		database = new(Database)
		database.Name = name
		database.DriverName = driverName
		database.DataSourceName = dataSourceName
		databases[name] = database
		err = database.Conn()
	} else {
		err = database.Ping()
	}
	return
}

type ParmaField struct {
	name string
	val  interface{}
}

type ParamsInterface interface {
	GetOrLen() int
	GetWhereLen() int
	GetSetLen() int
	GetOr() []ParmaField
	GetWhere() []ParmaField
	GetSet() []ParmaField
	GetFields() []string
	GetOrder() []string
	GetLimit() [2]int
	GetTableName() string
}

/**
 传参解析
**/
type Params struct {
	connname  string
	tbname    string
	where     []ParmaField
	or        []ParmaField
	set       []ParmaField
	fields    []string
	order     []string
	limit     [2]int
	insertsql string
	hasRow    bool
}

func (self Params) GetWhereLen() int {
	return len(self.where)
}
func (self Params) GetOrLen() int {
	return len(self.or)
}
func (self Params) GetSetLen() int {
	return len(self.set)
}

func (self Params) GetWhere() []ParmaField {
	return self.where
}
func (self Params) GetOr() []ParmaField {
	return self.or
}
func (self Params) GetSet() []ParmaField {
	return self.set
}

func (self Params) GetFields() []string {
	return self.fields
}
func (self Params) GetOrder() []string {
	return self.order
}
func (self Params) GetLimit() [2]int {
	return self.limit
}
func (self *Params) Init() {
	self.connname = ""
	self.hasRow = false
	self.where = self.where[len(self.where):]

	self.or = self.or[len(self.or):]

	self.set = self.set[len(self.set):]
	self.fields = self.fields[len(self.fields):]
	self.order = self.order[len(self.order):]
}

func (self *Params) SetTable(tbname string) {
	self.tbname = tbname

}

func (self *Params) SetField(fields ...string) {
	self.fields = fields
}

func (self *Params) Filter(name string, val interface{}) *Params {

	self.where = append(self.where, ParmaField{name, val})

	return self
}
func (self *Params) FilterOr(name string, val interface{}) *Params {
	self.or = append(self.or, ParmaField{name, val})
	return self
}

// 添加修改
func (self *Params) Change(name string, val interface{}) {
	self.set = append(self.set, ParmaField{name, val})
}
func (self *Params) Limit(page, step int) *Params {
	self.limit[0] = page
	self.limit[1] = step
	return self
}

func (self *Params) Db(name string) *Params {
	self.connname = name
	return self
}

func (self *Params) getReadConnect() (*Database, ModuleToSql) {
	db_connect_name := readDbConnectName
	if self.connname != "" {
		db_connect_name = self.connname
	}
	if db, ok := databases[db_connect_name]; ok {
		return db, driversql[db.DriverName](*self)

	} else {
		panic("database name '" + db_connect_name + "' not exists!")
	}
}
func (self *Params) getWriteConnect() (*Database, ModuleToSql) {
	db_connect_name := writeDbConnectName
	if self.connname != "" {
		db_connect_name = self.connname
	}
	if db, ok := databases[db_connect_name]; ok {
		return db, driversql[db.DriverName](*self)
	} else {
		panic("database name '" + db_connect_name + "' not exists!")
	}
}

func (self *Params) All() (rows *sql.Rows, err error) {
	//rows, err = self.db.Query(self.execSelect())
	//	self.stmt, err = self.db.Prepare()
	db, query := self.getReadConnect()
	sqls, val := query.Select()
	if debug_sql {
		Debug.Println("select all ", sqls, val)
	}
	rows, err = db.Query(sqls, val...)
	if err != nil {
		Error.Println(err)
	}

	return
}

func (self *Params) One(vals ...interface{}) error {
	//rows, err = self.db.Query(self.execSelect())
	//	self.stmt, err = self.db.Prepare()
	db, query := self.getReadConnect()
	sqls, val := query.Select()
	err := db.QueryRow(sqls, val...).Scan(vals...)
	if debug_sql {
		Debug.Println("select One ", sqls, val, err)
	}
	switch {
	case err == sql.ErrNoRows:
		return err
	case err != nil:
		return err
	default:
		self.hasRow = true
	}

	return nil
}
func (self *Params) Delete() (res sql.Result, err error) {

	db, query := self.getWriteConnect()
	sqls, val := query.Delete()
	if debug_sql {
		Debug.Println("delete  ", sqls, val)
	}
	var stmt *sql.Stmt
	stmt, err = db.Prepare(sqls)
	if err == nil {
		defer stmt.Close()
	} else {
		Error.Println(err)
		return
	}
	res, err = stmt.Exec(val...)
	if err != nil {
		Error.Println(err)
	}

	return
}

func (self *Params) Count() (int64, error) {
	db, query := self.getReadConnect()

	sqls, val := query.Count()
	if debug_sql {
		Debug.Println("count  ", sqls, val)
	}
	row := db.QueryRow(sqls, val...)

	var c int64
	if err := row.Scan(&c); err == nil {
		return c, nil
	} else {
		return 0, err
	}

	return 0, nil
}

func (self *Params) Save() (bool, int64, error) {
	db, query := self.getWriteConnect()
	defer func() {
		self.set = self.set[len(self.set):]
	}()
	var err error
	var stmt *sql.Stmt
	var res sql.Result
	//var n int64
	//if n , err= self.Count();err == nil && n >0
	if self.hasRow {
		sqls, val := query.Update()
		if debug_sql {
			Debug.Println("save update ", sqls, val)
		}
		stmt, err = db.Prepare(sqls)
		if err == nil {
			defer stmt.Close()
		} else {
			return false, 0, err
		}
		res, err = stmt.Exec(val...)

		if err != nil {
			return false, 0, err
		}
		a, b := res.RowsAffected()
		return false, a, b
	} else {
		sqls, val := query.Insert()
		if debug_sql {
			Debug.Println("save insert ", sqls, val)
		}
		stmt, err = db.Prepare(sqls)
		if err == nil {
			defer stmt.Close()
		} else {
			panic(err)
		}
		res, err = stmt.Exec(val...)
		if err != nil {
			return true, 0, err
		}
		a, b := res.LastInsertId()
		self.hasRow = true
		return true, a, b
	}

}

func (self Params) GetTableName() string {
	tbname := ""
	if tb := strings.Split(self.tbname, "."); len(tb) > 1 {
		tbname = fmt.Sprintf("`%s`.`%s`", tb[0], tb[1])
	} else {
		tbname = "`" + self.tbname + "`"
	}
	return tbname
}
