package orm

import (
	"database/sql"
	"reflect"
)

type Rows interface {
	Next() bool
	Scan(Module) error
	Close() error
}

type CacheRows struct {
	cache  Cache
	keys   []string
	dbName string
	index  int
	key    string
}

func (self *CacheRows) Next() bool {
	defer func() {
		self.index = self.index + 1
		self.keys = self.keys[len(self.keys):]
	}()

	if len(self.keys) > self.index {
		self.key = self.keys[self.index]
		return true
	} else {
		return false
	}
}

func (self *CacheRows) Close() error {
	self.keys = nil
	self.cache = nil
	return nil
}

func (self *CacheRows) Scan(mode Module) error {
	typ := reflect.TypeOf(mode).Elem()
	val := reflect.ValueOf(mode).Elem()

	err := self.cache.key2Mode(self.key, typ, val)
	if err != nil {
		Error.Println(err)
		return err
	}
	m := CacheHook{}
	m.Objects(val.Addr().Interface().(Module), self.dbName).Existed()
	val.FieldByName("CacheHook").Set(reflect.ValueOf(m))

	return nil
}

type ModeRows struct {
	rows   *sql.Rows
	val    []interface{}
	dbName string
}

func (self *ModeRows) Next() bool {
	return self.rows.Next()
}

func (self *ModeRows) Close() error {
	defer func() {
		self.rows = nil
	}()
	err := self.rows.Close()
	if err != nil {
		return err
	}
	return nil
}

func (self *ModeRows) Scan(mode Module) (err error) {
	if self.val == nil {
		self.val = []interface{}{}
	}
	self.val = self.val[len(self.val):]
	defer func() {
		self.val = self.val[len(self.val):]
	}()
	m := reflect.ValueOf(mode).Elem()
	typ := reflect.TypeOf(mode).Elem()
	for i := 0; i < m.NumField(); i++ {
		if name := typ.Field(i).Tag.Get("field"); len(name) > 0 {
			self.val = append(self.val, m.Field(i).Addr().Interface())
		}
	}
	err = self.rows.Scan(self.val...)
	if err != nil {
		return
	}
	if field := m.FieldByName("CacheHook"); field.IsValid() {
		obj := CacheHook{}
		obj.Objects(m.Addr().Interface().(Module), self.dbName).Existed()
		obj.SaveToCache()
		field.Set(reflect.ValueOf(obj))

	} else {

		obj := DBHook{} //Object(m.Interface().(Module))
		obj.Objects(m.Addr().Interface().(Module), self.dbName).Existed()
		m.FieldByName("DBHook").Set(reflect.ValueOf(obj))
	}
	return
}
