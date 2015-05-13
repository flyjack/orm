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
}

type CacheModuleInteerface interface {
	Objects(Module) CacheModuleInteerface
	Ca(interface{}) CacheModuleInteerface //一致性hash 默认处理方式
	Db(string) CacheModuleInteerface      //数据库连接
	Filter(name string, val interface{}) CacheModuleInteerface
	GetCacheKey() string
	Incrby(string, int64) (int64, error)
	Incry(string) (int64, error)
	Set(string, interface{}) error
	Save() (bool, int64, error)
	One() error
	SaveToCache() error
	All() ([]interface{}, error)
	AllCache() ([]interface{}, error)
	DoesNotExist() error
}

type CacheModule struct {
	Cache
	Object
	cachekey      string
	CacheFileds   []string
	CacheNames    []string
	cache_prefix  string
	cache_address string
	modefield     map[string]interface{}
}

func (self *CacheModule) Objects(mode Module) *CacheModule {
	self.CacheFileds = []string{}
	self.CacheNames = []string{}
	self.Object.Objects(mode)
	self.Lock()
	defer self.Unlock()
	typeOf := reflect.TypeOf(self.mode).Elem()
	valOf := reflect.ValueOf(self.mode).Elem()
	self.modefield = make(map[string]interface{}, typeOf.NumField())

	self.Cache = nil
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		if name := field.Tag.Get("cache"); len(name) > 0 {
			self.CacheFileds = append(self.CacheFileds, field.Tag.Get("field"))
			self.CacheNames = append(self.CacheNames, name)
		}
		if prefix := field.Tag.Get("cache_prefix"); len(prefix) > 0 {
			self.cache_prefix = prefix
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

func (self *CacheModule) Db(name string) *CacheModule {
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

func (self *CacheModule) Ca(key interface{}) *CacheModule {
	if use_hash_cache {
		self.cache_address, self.Cache = GetCacheConn(key)
		if debug_sql {
			Debug.Println("Change cache address  redis ", self.cache_address)
		}
	}
	return self
}

func (self *CacheModule) GetCacheKey() string {

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
func (self *CacheModule) setModeField(field string, val interface{}) error {

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
			Error.Println("(CacheModule) setModeField", field, item.Type().Kind())
			return errors.New("(CacheModule) setModeField type error ")
		}
		return nil
	} else {
		return errors.New("(CacheModule) setModeField field not exist")
	}
}

func (self *CacheModule) Incrby(key string, val int64) (ret int64, err error) {
	if self.cachekey == "" {
		self.cachekey = self.GetCacheKey()
	}

	ret, _ = self.Cache.Hincrby(self.cachekey, key, val)
	if val != 0 {
		if val >= 0 {
			self.Object.Change(key+"__add", val)
		} else {
			self.Object.Change(key+"__sub", val)
		}

		err = self.setModeField(key, ret)
	}
	//self.modefield[key] = ret
	//go self.Object.Save()
	return
}

func (self *CacheModule) Incry(key string) (val int64, err error) {
	val, err = self.Incrby(key, 1)

	return
}

func (self *CacheModule) Set(key string, value interface{}) (err error) {

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
	self.Object.Change(key, val)
	//self.modefield[key] = value
	//go self.Object.Change(key, val).Save()

	return
}

func (self *CacheModule) Save() (isnew bool, id int64, err error) {
	/// 实现， 直接通过 self.xxx= xxx 这样的数据变动支持
	/*
		valof := reflect.ValueOf(self.mode).Elem()
		typof := reflect.TypeOf(self.mode).Elem()
		for i := 0; i < valof.NumField(); i++ {
			if typof.Field(i).Tag.Get("field") != "" &&
				valof.Field(i).Interface() != self.modefield[typof.Field(i).Name] {

				switch valof.Field(i).Kind() {
				case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
					self.Set(typof.Field(i).Name, valof.Field(i).Uint())
				case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
					self.Set(typof.Field(i).Name, valof.Field(i).Int())
				case reflect.String:
					self.Set(typof.Field(i).Name, valof.Field(i).String())
				}

				Debug.Println("============= \\ ", self.cachekey, typof.Field(i).Name, valof.Field(i).Interface(), self.modefield[typof.Field(i).Name])
			}
		}
		defer func() {
			self.modefield = nil
		}()
	*/
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

func (self *CacheModule) Filter(name string, val interface{}) *CacheModule {
	self.Object.Filter(name, val)
	return self
}
func (self *CacheModule) Orderby(order ...string) *CacheModule {
	self.Object.Orderby(order...)
	return self
}
func (self *CacheModule) Limit(page, step int) *CacheModule {
	self.Object.Limit(page, step)
	return self
}
func (self *CacheModule) AllOnCache(out interface{}) error {
	defer func() {
		val := reflect.ValueOf(out).Elem()
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).Elem().FieldByName("CacheModule").FieldByName("Cache").IsNil() {
				m := CacheModule{}
				m.Objects(val.Index(i).Addr().Interface().(Module)).Existed()
				val.Index(i).Elem().FieldByName("CacheModule").Set(reflect.ValueOf(m))
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
func (self *CacheModule) All(out interface{}) error {

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
				if val.Index(i).Elem().FieldByName("CacheModule").FieldByName("Cache").IsNil() {
					m := CacheModule{}
					m.Objects(val.Index(i).Interface().(Module)).Existed()
					m.SaveToCache()
					val.Index(i).Elem().FieldByName("CacheModule").Set(reflect.ValueOf(m))
				}
			}

			return nil
		} else {
			Error.Println(err)
			return err
		}
	}
}
func (self *CacheModule) Delete() (err error) {
	err = self.DeleteOnCache()
	if _, err = self.Object.Delete(); err != nil {
		return
	}

	return
}
func (self *CacheModule) DeleteOnCache() error {
	if len(self.cachekey) <= 0 {
		self.cachekey = self.getKey()
	}
	if _, err := self.Del(self.cachekey); err != nil {
		return err
	}

	return nil
}
func (self *CacheModule) OneOnCache() error {
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
	for i := 0; i < val.NumField(); i++ {
		if b, err := self.Cache.Hget(key, typ.Field(i).Name); err == nil {
			switch val.Field(i).Kind() {
			case reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16:
				id, _ := strconv.ParseUint(string(b), 10, 64)
				val.Field(i).SetUint(id)
			case reflect.Int32, reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16:
				id, _ := strconv.ParseInt(string(b), 10, 64)
				val.Field(i).SetInt(id)
			case reflect.Float32, reflect.Float64:
				id, _ := strconv.ParseFloat(string(b), 64)
				val.Field(i).SetFloat(id)
			case reflect.String:
				val.Field(i).SetString(string(b))
			case reflect.Bool:
				id, _ := strconv.ParseBool(string(b))
				val.Field(i).SetBool(id)
			default:
				switch val.Field(i).Interface().(type) {
				case time.Time:
					if time, e := time.Parse(time.RFC1123Z, string(b)); e == nil {
						val.Field(i).Set(reflect.ValueOf(time))
					}
				}
			}
			if index := typ.Field(i).Tag.Get("index"); len(index) > 0 {
				self.Object.Filter(typ.Field(i).Name, val.Field(i).Interface())
			}
		}
	}
	self.hasRow = true
	return nil
}

func (self *CacheModule) One() error {

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
func (self *CacheModule) CountOnCache() (int64, error) {
	if keys, err := self.Cache.Keys(self.getKey()); err == nil {
		return int64(len(keys)), nil
	} else {
		return 0, err
	}
}
func (self *CacheModule) Count() (int64, error) {
	if keys, err := self.Cache.Keys(self.getKey()); err == nil && len(keys) > 0 {
		return int64(len(keys)), nil
	}
	return self.Object.Count()
}

func (self *CacheModule) getKey() string {
	key := ""
	if len(self.where) > 0 {
		key = self.where2Key()
	} else {
		key = self.GetCacheKey()
	}
	return key
}
func (self *CacheModule) where2Key() string {
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

func (self *CacheModule) fieldToByte(value interface{}) (str []byte) {
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

func (self *CacheModule) key2Mode(key string) reflect.Value {
	typ := reflect.TypeOf(self.mode).Elem()
	val := reflect.New(typ).Elem()
	for i := 0; i < typ.NumField(); i++ {
		if b, err := self.Cache.Hget(key, typ.Field(i).Name); err == nil {
			switch val.Field(i).Kind() {
			case reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16:
				id, _ := strconv.ParseUint(string(b), 10, 64)
				val.Field(i).SetUint(id)
			case reflect.Int32, reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16:
				id, _ := strconv.ParseInt(string(b), 10, 64)
				val.Field(i).SetInt(id)
			case reflect.Float32, reflect.Float64:
				id, _ := strconv.ParseFloat(string(b), 64)
				val.Field(i).SetFloat(id)
			case reflect.String:
				val.Field(i).SetString(string(b))
			case reflect.Bool:
				id, _ := strconv.ParseBool(string(b))
				val.Field(i).SetBool(id)
			default:
				switch val.Field(i).Interface().(type) {
				case time.Time:
					//str = append(str, []byte(values.(time.Time).Format(time.RFC1123Z))...)
					if time, e := time.Parse(time.RFC1123Z, string(b)); e == nil {
						val.Field(i).Set(reflect.ValueOf(time))
					}
				}
			}
		}
	}
	mode := CacheModule{}
	mode.Objects(val.Addr().Interface().(Module)).Existed()
	val.FieldByName("CacheModule").Set(reflect.ValueOf(mode))
	return val
}

func (self CacheModule) SaveToCache() error {
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
