package orm

import (
	"fmt"
	"strings"
)

/*
go get github.com/lib/pq

import (
	"github.com/ablegao/orm"
	_ "github.com/lib/pq"
)
// http://godoc.org/github.com/lib/pq
func main(){
	orm.NewDatabase("default" ,"postgres" ,"user=pqgotest dbname=pqgotest sslmode=verify-full" )
}
*/

type PostgressModeToSql struct {
	Params ParamsInterface
}

func (self PostgressModeToSql) Instance(param ParamsInterface) {
	self.Params = param
}

/*where

where 条件:
__exact        精确等于 like 'aaa'
 __iexact    精确等于 忽略大小写 ilike 'aaa'
 __contains    包含 like '%aaa%'
 __icontains    包含 忽略大小写 ilike '%aaa%'，但是对于sqlite来说，contains的作用效果等同于icontains。
__gt    大于
__gte    大于等于
__ne    不等于
__lt    小于
__lte    小于等于
__in     存在于一个list范围内
__startswith   以...开头
__istartswith   以...开头 忽略大小写
__endswith     以...结尾
__iendswith    以...结尾，忽略大小写
__range    在...范围内
__year       日期字段的年份
__month    日期字段的月份
__day        日期字段的日
__isnull=True/False
**/
func (self PostgressModeToSql) _w(a string) string {
	typ := ""
	if bb := strings.Split(a, "__"); len(bb) > 1 {
		a = bb[0]
		typ = strings.ToLower(bb[1])
	}
	patten := ""
	switch typ {
	case "iexact":
		patten = "`%s` ilike '?'"
	case "exact":
		patten = "`%s`  like '?' "
	case "contains":
		patten = "`%s` like '%?%'"
	case "icontains":
		patten = "`%s` ilike '%?%'"
	case "startswith":
		patten = "`%s` like '?%' "
	case "istartswith":
		patten = "`%s` ilike '?%' "
	case "endswith":
		patten = "`%s` like '%?' "
	case "iendswith":
		patten = "`%s` ilike '%?' "

	case "gt":
		patten = "`%s`>?"
	case "gte":
		patten = "`%s`>=?"
	case "lt":
		patten = "`%s`<?"
	case "lte":
		patten = "`%s`<=?"
	case "ne":
		patten = "`%s`<>?"
	case "add":
		return fmt.Sprintf("`%s`=`%s`+?", a, a)
	case "sub":
		return fmt.Sprintf("`%s`=`%s`-?", a, a)
	case "mult":
		return fmt.Sprintf("`%s`=`%s`*?", a, a)
	case "div":
		return fmt.Sprintf("`%s`=`%s`/?", a, a)
	case "asc":
		patten = "`%s` ASC"
	case "desc":
		patten = "`%s` DESC"
	default:
		patten = "`%s`=?"
	}
	return fmt.Sprintf(patten, a)
}
func (self PostgressModeToSql) _where() (sql string, val []interface{}) {
	whereLen := self.Params.GetWhereLen()
	orLen := self.Params.GetOrLen()

	where := make([]string, whereLen)
	or := make([]string, orLen)

	val = make([]interface{}, whereLen+orLen)

	i := 0
	for _, w := range self.Params.GetWhere() {

		//where = append(where, self.Params._w(w.name))
		where[i] = self._w(w.name)
		val[i] = w.val
		i = i + 1
	}

	for _, w := range self.Params.GetOr() {
		or[i] = self._w(w.name)
		val[i] = w.val
		i = i + 1
	}

	sql = ""
	switch {
	case whereLen > 0 && orLen > 0:
		sql = sql + " WHERE " + strings.Join(where, " AND ") + " OR " + strings.Join(or, " OR ")
	case whereLen > 0 && orLen == 0:
		sql = sql + " WHERE " + strings.Join(where, " AND ")
	case orLen > 0 && whereLen == 0:
		sql = sql + " WHERE " + strings.Join(or, " OR ")
	}
	return
}
func (self PostgressModeToSql) _set() (sql string, val []interface{}) {
	sets := self.Params.GetSet()
	l := len(sets)
	set := make([]string, l)
	val = make([]interface{}, l)
	for i, v := range sets {
		set[i] = self._w(v.name)
		val[i] = v.val
	}
	sql = " SET " + strings.Join(set, ",")
	return
}
func (self PostgressModeToSql) Insert() (sql string, val []interface{}) {
	sql, val = self._set()
	sql = fmt.Sprintf("INSERT INTO  %s %s ",
		self.Params.GetTableName(),
		sql,
	)
	return
}
func (self PostgressModeToSql) Update() (sql string, val []interface{}) {
	sql, val = self._set()
	sql = fmt.Sprintf("UPDATE  %s %s ",
		self.Params.GetTableName(),
		sql,
	)
	s, v := self._where()
	sql = sql + s
	val = append(val, v...)
	return
}

func (self PostgressModeToSql) Delete() (sql string, val []interface{}) {
	sql, val = self._where()

	sql = fmt.Sprintf("DELETE FROM %s %s ",
		self.Params.GetTableName(),
		sql,
	)

	return
}
func (self PostgressModeToSql) Select() (sql string, val []interface{}) {

	sql, val = self._where()
	sql = fmt.Sprintf("SELECT `%s` FROM %s  %s",
		strings.Join(self.Params.GetFields(), "`,`"),
		self.Params.GetTableName(),
		sql,
	)
	order := self.Params.GetOrder()
	if len(order) > 0 {
		sql = sql + " ORDER BY "
		ret := make([]string, len(order))
		for id, v := range order {
			ret[id] = self._w(v)
		}
		sql = sql + strings.Join(ret, ",")
	}
	limit := self.Params.GetLimit()
	if limit != NULL_LIMIT {

		sql = sql + fmt.Sprintf(" LIMIT %d OFFSET %d", limit[1], (limit[0]-1)*limit[1])
	}

	return
}
func (self PostgressModeToSql) Count() (sql string, val []interface{}) {
	sql, val = self._where()
	sql = fmt.Sprintf("SELECT COUNT(*) FROM %s  %s ",
		self.Params.GetTableName(),
		sql,
	)
	return
}
