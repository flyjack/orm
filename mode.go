// model解析支持

package orm

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"
)

var ErrDoesNotExist = errors.New("DoesNotExist")

type Module interface {
	GetTableName() string
}

type FuncParam struct {
	name string
	val  func(interface{}) bool
}

type Object struct{ DBHook }

type DBHook struct {
	sync.RWMutex
	Params
	mode      Module
	funcWhere []FuncParam
	dbName    string
}

func (self *DBHook) DoesNotExist() error {
	return ErrDoesNotExist
}

func (self *DBHook) Objects(mode Module, params ...string) *DBHook {
	self.Lock()
	defer self.Unlock()
	if len(params) == 1 && len(params[0]) > 1 {
		self.dbName = params[0]
		self.SetTable(self.dbName + "." + mode.GetTableName())
	} else {
		self.SetTable(mode.GetTableName())
	}

	self.Init()
	self.funcWhere = self.funcWhere[len(self.funcWhere):]
	typ := reflect.TypeOf(mode).Elem()
	vals := []string{}

	for i := 0; i < typ.NumField(); i++ {
		if field := typ.Field(i).Tag.Get("field"); len(field) > 0 {
			vals = append(vals, field)
		}
	}
	self.SetField(vals...)
	self.mode = mode
	return self
}

func (self *DBHook) Existed() *DBHook {
	self.Lock()
	defer self.Unlock()
	self.hasRow = true
	return self
}

//修改数据
// name 结构字段名称
// val 结构数据
func (self *DBHook) Set(name string, val interface{}) *DBHook {
	self.Lock()
	defer self.Unlock()
	typ := reflect.TypeOf(self.mode).Elem()
	fieldName := strings.Split(name, "__")
	if field, ok := typ.FieldByName(fieldName[0]); ok && len(field.Tag.Get("field")) > 0 {
		name := field.Tag.Get("field")
		if len(fieldName) > 1 {
			name = name + "__" + fieldName[1]
		}
		self.Params.Change(name, val)
	}
	return self
}

func (self *DBHook) Change(name string, val interface{}) *DBHook {
	return self.Set(name, val)
}

//条件筛选
// name 结构字段名称
// val 需要过滤的数据值
func (self *DBHook) Filter(name string, val interface{}) *DBHook {
	self.Lock()
	defer self.Unlock()
	switch val.(type) {
	case func(interface{}) bool:
		self.funcWhere = append(self.funcWhere, FuncParam{name, val.(func(interface{}) bool)})
	default:
		typ := reflect.TypeOf(self.mode).Elem()
		fieldName := strings.Split(name, "__")
		if field, ok := typ.FieldByName(fieldName[0]); ok && len(field.Tag.Get("field")) > 0 {
			name := field.Tag.Get("field")
			if len(fieldName) > 1 {
				name = name + "__" + fieldName[1]
			}
			self.Params.Filter(name, val)
		}
	}

	return self
}
func (self *DBHook) FilterOr(name string, val interface{}) *DBHook {
	self.Lock()
	defer self.Unlock()
	typ := reflect.TypeOf(self.mode).Elem()
	fieldName := strings.Split(name, "__")
	if field, ok := typ.FieldByName(fieldName[0]); ok && len(field.Tag.Get("field")) > 0 {
		name := field.Tag.Get("field")
		if len(fieldName) > 1 {
			name = name + "__" + fieldName[1]
		}
		self.Params.FilterOr(name, val)
	}
	return self
}

// Filter 的一次传入版本 ， 不建议使用 , 因为map 循序不可控
func (self *DBHook) Filters(filters map[string]interface{}) *DBHook {
	for k, v := range filters {
		self.Filter(k, v)
	}
	return self
}

// Order by 排序 ，
// Field__asc Field__desc
func (self *DBHook) Orderby(names ...string) *DBHook {
	typ := reflect.TypeOf(self.mode).Elem()
	for i, name := range names {
		fieldName := strings.Split(name, "__")
		if field, ok := typ.FieldByName(fieldName[0]); ok && len(field.Tag.Get("field")) > 0 {
			if name = field.Tag.Get("field"); len(name) > 0 {
				name = name + "__" + fieldName[1]
				names[i] = name
			}
		}
	}
	self.Params.order = names
	return self
}

// 分页支持
func (self *DBHook) Limit(page, steq int) *DBHook {
	self.Lock()
	defer self.Unlock()
	self.Params.limit = [2]int{page, steq}
	return self
}

//选择数据库
func (self *DBHook) Db(name string) *DBHook {
	self.Params.Db(name)
	return self
}

// 计算数量
func (self *DBHook) Count() (int64, error) {
	self.RLock()
	defer self.RUnlock()
	return self.Params.Count()
}

//删除数据
func (self *DBHook) Delete() (int64, error) {
	self.Lock()
	defer self.Unlock()
	self.autoWhere()
	if len(self.Params.where) > 0 {
		res, err := self.Params.Delete()
		if OpenSyncDelete {
			return 0, nil
		}

		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	} else {
		return 0, errors.New("where params is 0")
	}

}

func (self DBHook) printModel(name string) {
	valus := reflect.ValueOf(self.mode).Elem()
	Debug.Println("PRINT MODE =======================================", name, " Start ")
	for i := 0; i < valus.NumField(); i++ {
		Debug.Println("PRINT MODE ", valus.Type().Field(i).Name, valus.Field(i).Interface())
	}
	Debug.Println("PRINT MODE =======================================", name, " END ")
}

//更新活添加
func (self *DBHook) Save() (bool, int64, error) {
	self.Lock()
	defer self.Unlock()
	valus := reflect.ValueOf(self.mode).Elem()
	fieldNum := valus.NumField()
	if len(self.Params.set) == 0 {
		for i := 0; i < fieldNum; i++ {
			typ := valus.Type().Field(i)
			val := valus.Field(i)
			if self.hasRow {
				if len(typ.Tag.Get("field")) > 0 && typ.Tag.Get("index") != "pk" {
					self.Params.Change(typ.Tag.Get("field"), val.Interface())
				}
			} else {
				if len(typ.Tag.Get("field")) > 0 {
					self.Params.Change(typ.Tag.Get("field"), val.Interface())
				}
			}
		}
	}

	self.autoWhere()
	isNew, id, err := self.Params.Save()
	if isNew && err == nil {
		for i := 0; i < fieldNum; i++ {
			typ := valus.Type().Field(i)
			val := valus.Field(i)
			if typ.Tag.Get("index") == "pk" {
				switch val.Kind() {
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					if val.Uint() == 0 {
						val.SetUint(uint64(id))
					}
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					if val.Int() == 0 {
						val.SetInt(id)
					}
				}
			}
		}
	}

	return isNew, id, err
}

func (self *DBHook) autoWhere() {
	valus := reflect.ValueOf(self.mode).Elem()
	if len(self.Params.where) == 0 {
		for i := 0; i < valus.NumField(); i++ {
			typ := valus.Type().Field(i)
			val := valus.Field(i)
			if len(typ.Tag.Get("field")) > 0 && typ.Tag.Get("index") == "pk" {
				switch val.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					if val.Int() > 0 {
						self.Params.Filter(typ.Tag.Get("field"), val.Int())
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					if val.Uint() > 0 {
						self.Params.Filter(typ.Tag.Get("field"), val.Uint())
					}
				case reflect.Float32, reflect.Float64:
					if val.Float() > 0.0 {
						self.Params.Filter(typ.Tag.Get("field"), val.Float())
					}
				case reflect.String:
					if len(val.String()) > 0 {
						self.Params.Filter(typ.Tag.Get("field"), val.String())
					}
				default:
					switch val.Interface().(type) {
					case time.Time:
						self.Params.Filter(typ.Tag.Get("field"), val.Interface())
					}

				}
			}
		}
	}

}
func (self *DBHook) Query() (Rows, error) {
	self.autoWhere()
	rows, err := self.Params.All()
	if err != nil {
		return nil, err
	}
	return &ModeRows{rows: rows, dbName: self.dbName}, nil
}

//查找数据
func (self *DBHook) All(out interface{}) error {
	self.Lock()
	defer self.Unlock()
	if out == nil {
		return errors.New("params can't nil ")
	}
	self.autoWhere()
	rows, err := self.Params.All()

	if err == nil {
		defer rows.Close()
		val := []interface{}{}
		value := reflect.ValueOf(out).Elem()

		for rows.Next() {
			m := reflect.New(reflect.TypeOf(self.mode).Elem()).Elem()
			val = val[len(val):]

			for i := 0; i < m.NumField(); i++ {
				if name := m.Type().Field(i).Tag.Get("field"); len(name) > 0 {
					val = append(val, m.Field(i).Addr().Interface())
				}
			}

			err = rows.Scan(val...)
			if err != nil {
				Error.Println(err)
				continue
			}
			//m.Field(0).MethodByName("Objects").Call([]reflect.Value{m.Addr()})
			obj := DBHook{} //DBHook(m.Interface().(Module))
			obj.Objects(m.Addr().Interface().(Module), self.dbName).Existed()
			m.FieldByName("DBHook").Set(reflect.ValueOf(obj))
			add := true
			for _, param := range self.funcWhere {

				if param.val(m.FieldByName(param.name).Interface()) == false {
					add = false
				}
			}
			if add == true {
				value.Set(reflect.Append(value, m.Addr()))
			}

		}
		return err
	} else {
		Error.Println(err)
		return err
	}

}

//提取一个数据
func (self *DBHook) One() error {
	self.RLock()
	defer self.RUnlock()
	self.autoWhere()
	valMode := reflect.ValueOf(self.mode).Elem()
	typeMode := reflect.TypeOf(self.mode).Elem()
	vals := []interface{}{}
	for i := 0; i < valMode.NumField(); i++ {
		if name := typeMode.Field(i).Tag.Get("field"); len(name) > 0 {
			//vals[i] = valMode.Field(i).Addr().Interface()
			vals = append(vals, valMode.Field(i).Addr().Interface())
		}
	}
	err := self.Params.One(vals...)
	if err == nil {
		self.where = self.where[len(self.where):]
		return nil
	} else {

		switch err {
		case sql.ErrNoRows:
			return ErrDoesNotExist
		default:
			return err
		}
	}
}

func (self *DBHook) Field(name string) reflect.Value {
	valMode := reflect.ValueOf(self.mode).Elem()
	return valMode.FieldByName(name)
}
