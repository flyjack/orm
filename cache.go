//Cache，用来沟通第一层cache 和同步数据到数据库

package orm

import (
	"errors"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"
)

var Debug = log.New(os.Stdout, "ORM-DEBUG ", log.Lshortfile|log.LstdFlags)
var Error = log.New(os.Stdout, "ORM-ERROR ", log.Lshortfile|log.LstdFlags)
var cache_prefix []byte = []byte("nado")
var cache_db = 0
var st []byte = []byte("*")

var (
	ErrKeyNotExist = errors.New("keys not exists")
)
var debug_sql bool = false

func SetCachePrefix(str string) {
	cache_prefix = []byte(str)

}
func GetCachePrefix() []byte {
	return cache_prefix
}
func SetDefaultCacheDb(db int) {
	cache_db = db
}

func SetDebug(b bool) {
	debug_sql = b
}

type Cache interface {
	Set(key string, b []byte) error
	Get(key string) ([]byte, error)
	Keys(key string) ([]string, error)
	//Incy(key string) (int64, error)
	Incrby(key string, n int64) (int64, error)
	Hset(key, field string, b []byte) (bool, error)
	Hmset(key string, maping interface{}) error
	Hget(key, field string) ([]byte, error)
	Hincrby(key, filed string, n int64) (int64, error)
	Exists(key string) (bool, error)
	Del(key string) (bool, error)
	key2Mode(key string, typ reflect.Type, val reflect.Value) error
}

type CacheModule struct{ CacheHook }

type CacheHook struct {
	Cache
	DBHook
	Object        *DBHook
	cachekey      string
	CacheFileds   []string
	CacheNames    []string
	cache_prefix  string
	cache_address string
	modefield     map[string]interface{}
}

func (self *CacheHook) Objects(mode Module, param ...string) *CacheHook {
	self.CacheFileds = []string{}
	self.CacheNames = []string{}
	self.DBHook.Objects(mode, param...)
	self.Object = &self.DBHook
	self.Lock()
	defer self.Unlock()
	typeOf := reflect.TypeOf(self.mode).Elem()
	valOf := reflect.ValueOf(self.mode).Elem()
	self.modefield = make(map[string]interface{}, typeOf.NumField())

	if len(param) == 1 && len(param[0]) > 0 {
		self.cache_prefix = param[0]
		//self.DbName = param[0]
	}

	self.Cache = nil
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		if name := field.Tag.Get("cache"); len(name) > 0 {
			self.CacheFileds = append(self.CacheFileds, field.Tag.Get("field"))
			self.CacheNames = append(self.CacheNames, name)
		}

		if prefix := field.Tag.Get("cache_prefix"); len(prefix) > 0 {
			self.cache_prefix = self.cache_prefix + prefix
		}

		//支持分布是hash key
		if use_hash_cache && field.Tag.Get("index") == "pk" && field.Tag.Get("distr") == "true" {
			self.cache_address, self.Cache = GetCacheConn(valOf.Field(i).Interface())
		}
		if name := field.Tag.Get("field"); name != "" {
			self.modefield[typeOf.Field(i).Name] = valOf.Field(i).Interface()
		}
	}
	if self.Cache == nil {
		self.Cache = GetCacheClient("default")
	}

	return self
}

func (self *CacheHook) Db(name string) *CacheHook {
	self.Params.Db(name)
	return self
}

func GetCacheConn(key interface{}) (address string, c Cache) {

	value := reflect.ValueOf(key)
	typeOf := reflect.TypeOf(key)
	b := []byte{}
	switch typeOf.Kind() {
	case reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16:
		b = strconv.AppendUint(b, value.Uint(), 10)
	case reflect.Int32, reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16:
		b = strconv.AppendInt(b, value.Int(), 10)
	case reflect.Float32, reflect.Float64:
		b = strconv.AppendFloat(b, value.Float(), 'f', 0, 64)
	case reflect.String:
		b = append(b, []byte(value.String())...)
	case reflect.Bool:
		b = strconv.AppendBool(b, value.Bool())
	}
	address = string(b)
	if use_hash_cache == false {
		return address, cacheconn
	}
	c = GetCacheClient(address)
	return
}

func (self *CacheHook) Ca(key interface{}) *CacheHook {
	if use_hash_cache {
		self.cache_address, self.Cache = GetCacheConn(key)
		if debug_sql {
			Debug.Println("Change cache address  redis ", self.cache_address)
		}
	}
	return self
}

func (self *CacheHook) GetCacheKey() string {

	value := reflect.ValueOf(self.mode).Elem()
	typeOf := reflect.TypeOf(self.mode).Elem()
	str := cache_prefix
	str = append(str, []byte(self.cache_prefix)...)

	for i := 0; i < value.NumField(); i++ {
		field := typeOf.Field(i)
		self.CacheFileds = append(self.CacheFileds, field.Name)
		if name := field.Tag.Get("cache"); len(name) > 0 {
			val := value.Field(i)
			str = append(str, []byte(":")...)
			str = append(str, self.fieldToByte(val.Interface())...)
			str = append(str, []byte(":"+name)...)
		}
	}
	return string(str)
}
func (self *CacheHook) setModeField(field string, val interface{}) error {

	typ := reflect.TypeOf(self.mode).Elem()

	if mfield, ok := typ.FieldByName(field); ok {
		value := reflect.ValueOf(self.mode).Elem()
		item := value.FieldByName(field)
		switch mfield.Type.Kind() {
		case reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16:

			item.SetUint(uint64(val.(int64)))
		case reflect.Int32, reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16:
			item.SetInt(val.(int64))
		case reflect.String:
			item.SetString(val.(string))

		default:
			Error.Println("(CacheHook) setModeField", field, item.Type().Kind())
			return errors.New("(CacheHook) setModeField type error ")
		}
		return nil
	} else {
		return errors.New("(CacheHook) setModeField field not exist")
	}
}

func (self *CacheHook) Incrby(key string, val int64) (ret int64, err error) {
	if self.cachekey == "" {
		self.cachekey = self.GetCacheKey()
	}

	ret, _ = self.Cache.Hincrby(self.cachekey, key, val)
	if val != 0 {

		self.Object.Change(key+"__add", val)

		err = self.setModeField(key, ret)
	}
	//self.modefield[key] = ret
	//go self.Object.Save()
	return
}

func (self *CacheHook) Incry(key string) (val int64, err error) {
	val, err = self.Incrby(key, 1)

	return
}

func (self *CacheHook) Set(key string, value interface{}) (err error) {

	b := []byte{}
	field := reflect.ValueOf(self.mode).Elem().FieldByName(key)

	val := reflect.ValueOf(value)

	switch field.Kind() {
	case reflect.Uint32, reflect.Uint64, reflect.Uint16, reflect.Uint8:
		b = strconv.AppendUint(b, val.Uint(), 10)
		field.SetUint(val.Uint())
	case reflect.String:
		b = append(b, []byte(val.String())...)
		field.SetString(val.String())

	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		b = strconv.AppendInt(b, val.Int(), 10)
		field.SetInt(val.Int())
	case reflect.Float64, reflect.Float32:
		b = strconv.AppendFloat(b, val.Float(), 'f', 0, 64)
		field.SetFloat(val.Float())
	case reflect.Bool:
		b = strconv.AppendBool(b, val.Bool())
		field.SetBool(val.Bool())

	default:
		switch value.(type) {
		case time.Time:
			field.Set(reflect.ValueOf(value))
			b = append(b, []byte(value.(time.Time).Format(time.RFC1123Z))...)
		default:
			Error.Println("undefined val type")
		}
	}
	if self.cachekey == "" {
		self.cachekey = self.GetCacheKey()
	}

	var over bool

	over, err = self.Cache.Hset(self.cachekey, key, b)
	if err != nil {
		return
	}
	if over == false {
		return errors.New(self.cachekey + " hset " + key + " error !")
	}
	self.Object.Change(key, value)
	//self.modefield[key] = value
	//go self.Object.Change(key, val).Save()

	return
}

func (self *CacheHook) Save() (isnew bool, id int64, err error) {
	/// 实现， 直接通过 self.xxx= xxx 这样的数据变动支持

	upgradecache := false
	if self.hasRow && len(self.set) == 0 {
		upgradecache = true
	}
	isnew, id, err = self.Object.Save()

	self.Lock()
	defer self.Unlock()
	if upgradecache == false {
		key := self.GetCacheKey()
		if i, err := self.Exists(key); err == nil && i == false {
			self.SaveToCache()
		}
	} else {
		self.SaveToCache()
	}
	return
}

func (self *CacheHook) Filter(name string, val interface{}) *CacheHook {
	self.Object.Filter(name, val)
	return self
}
func (self *CacheHook) Orderby(order ...string) *CacheHook {
	self.Object.Orderby(order...)
	return self
}
func (self *CacheHook) Limit(page, step int) *CacheHook {
	self.Object.Limit(page, step)
	return self
}

func (self *CacheHook) Query() (Rows, error) {
	key := self.getKey()
	keys, err := self.Keys(key)
	if err != nil {
		return nil, err
	}
	if len(keys) <= 0 {
		//return nil, errors.New(key + " not found")
		rows, err := self.Object.Query()
		return rows, err
	} else {
		sort.Sort(sort.StringSlice(keys))
		if self.limit != NULL_LIMIT {
			page := (self.limit[0] - 1) * self.limit[1]
			step := self.limit[0] * self.limit[1]
			if step > len(keys) {
				step = len(keys)
			}
			keys = keys[page:step]
		}

		return &CacheRows{keys: keys, dbName: self.DbName, index: 0, cache: self.Cache}, nil
	}

}

func (self *CacheHook) AllOnCache(out interface{}) error {
	defer func() {
		val := reflect.ValueOf(out).Elem()
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).Elem().FieldByName("CacheHook").FieldByName("Cache").IsNil() {
				m := CacheHook{}
				m.Objects(val.Index(i).Addr().Interface().(Module), self.DbName).Existed()
				val.Index(i).Elem().FieldByName("CacheHook").Set(reflect.ValueOf(m))
			}
		}
	}()
	key := self.getKey()
	if keys, err := self.Keys(key); err == nil && len(keys) > 0 {
		//(keys)
		sort.Sort(sort.StringSlice(keys))
		if self.limit != NULL_LIMIT {
			page := (self.limit[0] - 1) * self.limit[1]
			step := self.limit[0] * self.limit[1]
			if step > len(keys) {
				step = len(keys)
			}
			value := reflect.ValueOf(out).Elem()
			if page < len(keys) {
				keys = keys[page:step]
				for _, k := range keys {
					//vals[i] = self.key2Mode(k)
					if mode := self.key2Mode(k); mode.IsValid() {
						add := true
						for _, param := range self.funcWhere {
							if param.val(mode.FieldByName(param.name).Interface()) == false {
								add = false
							}
						}
						if add {
							value.Set(reflect.Append(value, mode.Addr()))
						}
					}
				}
			}
		} else {
			value := reflect.ValueOf(out).Elem()
			for _, k := range keys {

				if mode := self.key2Mode(k); mode.IsValid() {

					add := true
					for _, param := range self.funcWhere {
						if param.val(mode.FieldByName(param.name).Interface()) == false {
							add = false
						}
					}
					if add {
						value.Set(reflect.Append(value, mode.Addr()))
					}
				}
			}

		}

		return nil
	} else {
		return err
	}
}
func (self *CacheHook) All(out interface{}) error {

	if err := self.AllOnCache(out); err == nil && reflect.ValueOf(out).Elem().Len() > 0 {
		return err
	} else {
		//self.Object.All()
		if debug_sql {
			Debug.Println("========================== not in cache ", err, out)
		}
		if err := self.Object.All(out); err == nil {

			val := reflect.ValueOf(out).Elem()
			for i := 0; i < val.Len(); i++ {
				if val.Index(i).Elem().FieldByName("CacheHook").FieldByName("Cache").IsNil() {
					m := CacheHook{}
					m.Objects(val.Index(i).Interface().(Module), self.DbName).Existed()
					m.SaveToCache()
					val.Index(i).Elem().FieldByName("CacheHook").Set(reflect.ValueOf(m))
				}
			}

			return nil
		} else {
			Error.Println(err)
			return err
		}
	}
}
func (self *CacheHook) Delete() (err error) {
	err = self.DeleteOnCache()
	if _, err = self.Object.Delete(); err != nil {
		return
	}

	return
}
func (self *CacheHook) DeleteOnCache() error {
	if len(self.cachekey) <= 0 {
		self.cachekey = self.getKey()
	}
	if _, err := self.Del(self.cachekey); err != nil {
		return err
	}

	return nil
}
func (self *CacheHook) OneOnCache() error {
	key := self.getKey()

	self.cachekey = key
	n, err := self.Cache.Exists(self.cachekey)
	if err != nil {
		if debug_sql {
			Error.Println(err)
		}

		return err
	}

	if debug_sql {
		Debug.Println("key ", self.cachekey, self.Cache, " is exists ", n)
	}
	if n == false {
		return ErrKeyNotExist
	}

	self.where = self.where[len(self.where):]
	val := reflect.ValueOf(self.mode).Elem()
	typ := reflect.TypeOf(self.mode).Elem()
	self.Cache.key2Mode(key, typ, val)

	for i := 0; i < val.NumField(); i++ {
		if index := typ.Field(i).Tag.Get("index"); len(index) > 0 {
			self.Object.Filter(typ.Field(i).Name, val.Field(i).Interface())
		}
	}
	self.hasRow = true
	return nil
}

func (self *CacheHook) One() error {

	if err := self.OneOnCache(); err != nil {
		//return errors.New("key " + key + " not exists!")
		err = self.Object.One()
		if err == nil {

			err = self.SaveToCache() //self.saveToCache(self.mode)
		}
		return err
	} else {
		if debug_sql {
			Debug.Println("内存命中", self.cachekey)
		}
	}
	return nil
}
func (self *CacheHook) CountOnCache() (int64, error) {
	if keys, err := self.Cache.Keys(self.getKey()); err == nil {
		return int64(len(keys)), nil
	} else {
		return 0, err
	}
}
func (self *CacheHook) Count() (int64, error) {
	if keys, err := self.Cache.Keys(self.getKey()); err == nil && len(keys) > 0 {
		return int64(len(keys)), nil
	}
	return self.Object.Count()
}

func (self *CacheHook) getKey() string {
	key := ""
	if len(self.where) > 0 {
		key = self.where2Key()
	} else {
		key = self.GetCacheKey()
	}
	return key
}
func (self *CacheHook) where2Key() string {
	str := cache_prefix
	str = append(str, []byte(self.cache_prefix)...)
	for index, field := range self.CacheFileds {
		for _, wh := range self.where {
			if wh.name == field {
				str = append(str, []byte(":")...)
				str = append(str, self.fieldToByte(wh.val)...)
				str = append(str, []byte(":"+self.CacheNames[index])...)
				goto NEXT
			}

		}
		str = append(str, []byte(":*:"+self.CacheNames[index])...)
	NEXT:
	}
	return string(str)
}

func (self *CacheHook) fieldToByte(value interface{}) (str []byte) {
	typ := reflect.TypeOf(value)
	val := reflect.ValueOf(value)
	switch typ.Kind() {
	case reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16:
		if val.Uint() <= 0 {
			str = append(str, st...)
		} else {
			str = strconv.AppendUint(str, val.Uint(), 10)
		}
	case reflect.Int32, reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16:
		if val.Int() <= 0 {
			str = append(str, st...)
		} else {
			str = strconv.AppendInt(str, val.Int(), 10)
		}
	case reflect.Float32, reflect.Float64:
		if val.Float() == 0.0 {
			str = append(str, st...)
		} else {
			str = strconv.AppendFloat(str, val.Float(), 'f', 0, 64)
		}
	case reflect.String:
		if val.Len() <= 0 {
			str = append(str, st...)
		} else {
			str = append(str, []byte(val.String())...)
		}
	case reflect.Bool:
		switch val.Bool() {
		case true:
			str = append(str, []byte("true")...)
		case false:
			str = append(str, []byte("false")...)
		}
	default:
		switch value.(type) {
		case time.Time:
			str = append(str, []byte(value.(time.Time).Format(time.RFC1123Z))...)
		}
	}
	return
}

func (self *CacheHook) key2Mode(key string) reflect.Value {
	typ := reflect.TypeOf(self.mode).Elem()
	val := reflect.New(typ).Elem()

	var err error
	err = self.Cache.key2Mode(key, typ, val)
	if err != nil {
		Error.Println(err.Error())
	}
	mode := CacheHook{}
	mode.Objects(val.Addr().Interface().(Module), self.DbName).Existed()
	val.FieldByName("CacheHook").Set(reflect.ValueOf(mode))
	return val
}

func (self CacheHook) SaveToCache() error {
	key := self.GetCacheKey()

	maping := map[string]interface{}{}
	vals := reflect.ValueOf(self.mode).Elem()
	typ := reflect.TypeOf(self.mode).Elem()
	for i := 0; i < vals.NumField(); i++ {
		field := typ.Field(i)
		if name := field.Tag.Get("field"); len(name) > 0 {
			if nocache := field.Tag.Get("no_cache"); len(nocache) == 0 {
				switch vals.Field(i).Interface().(type) {
				case time.Time:
					maping[field.Name] = vals.Field(i).Interface().(time.Time).Format(time.RFC1123Z)
				default:
					maping[field.Name] = vals.Field(i).Interface()
				}
			}
		}
		//补充一个仅存在于cache中的字段。
		if name := field.Tag.Get("cache_only_field"); len(name) > 0 {
			switch vals.Field(i).Interface().(type) {
			case time.Time:
				maping[field.Name] = vals.Field(i).Interface().(time.Time).Format(time.RFC1123Z)
			default:
				maping[field.Name] = vals.Field(i).Interface()
			}
		}
	}
	err := self.Cache.Hmset(key, maping)
	if debug_sql {
		Debug.Println("转储倒内存", key, maping, err, self.Cache)
	}
	return nil
}
