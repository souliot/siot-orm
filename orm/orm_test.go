package orm

import (
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
)

type Logs struct {
	Id       string `orm:"pk" bson:"id"`
	Ltype    string `orm:"column(type)" bson:"type"`
	UserName string `orm:"column(username)"  bson:"username"`
}

func (m *Logs) TableName() string {
	return "log"
}

func init() {

	RegisterModel(new(Logs))

	// RegisterDriver("mongo", DRMongo)
	// RegisterDataBase("default", "mongo", "mongodb://yapi:abcd1234@192.168.50.200:27017/yapi", true)
	RegisterDriver("clickhouse", DRClickHouse)
	RegisterDataBase("default", "clickhouse", "tcp://192.168.0.8:9000?username=default&password=watrix888&database=test&read_timeout=10&write_timeout=20", true)
	// RegisterDataBase("default", "mysql", "root:watrix@/businessserver?charset=utf8", true)
	// RegisterDataBase("default", "mongo", "mongodb://yapi:abcd1234@vm:27017/yapi")

}

var (
	l = Logs{
		Id:       "asd",
		UserName: "linleizhou1234",
		Ltype:    "group",
	}
)

func TestRead(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	err := o.Read(&l, "UserName")
	t.Log(err, l)
}
func TestReadOrCreate(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	c, id, err := o.ReadOrCreate(&l, "UserName")
	t.Log(c, id, err, l)
}

func TestInsert(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	id, err := o.Insert(&l)
	t.Log(id, err)
}

func TestInsertMulti(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	ls := []Logs{}
	ls = append(ls, l)
	ls = append(ls, l)
	id, err := o.InsertMulti(100, ls)
	t.Log(id, err)
}

func TestUpdate(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	l.Id = "asd"
	l.Ltype = "group3"
	id, err := o.Update(&l, "Ltype")
	t.Log(id, err, l)
}

func TestDelete(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	l.Id = "asd"
	l.Ltype = "group3"
	cnt, err := o.Delete(&l, "Ltype")
	t.Log(cnt, err)
}

func TestQsOne(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	err := qs.Filter("username", "asda").One(&l, "username", "type")
	t.Log(err, l)
}

func TestQsAll(t *testing.T) {
	o := NewOrm()
	o.Using("default")
	var ls []Logs
	qs := o.QueryTable("log")
	err := qs.Filter("username__contains", "as").OrderBy("-id", "Ltype").Limit(2, 0).All(&ls)
	// num, err := qs.All(&ls)
	t.Log(err, ls)
}

func TestQsCount(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	num, err := qs.Filter("username__contains", "as").Count()
	// num, err := qs.Count()
	t.Log(num, err)
}
func TestQsUpdate(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	num, err := qs.Filter("_id", "5e7431f78c1b4111312cce2d").Update(MgoSet, Params{
		"type": "group",
	})
	t.Log(num, err)
}
func TestQsDelete(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	num, err := qs.Filter("type", "group3").Delete()
	t.Log(num, err)
}
func TestQsIndexList(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	indexes, err := qs.IndexView().List()
	t.Log(indexes, err)
}
func TestQsIndexCreateOne(t *testing.T) {
	o := NewOrm()
	o.Using("default")
	qs := o.QueryTable("log")

	index := Index{}
	index.Keys = []string{"-username", "_id"}
	index.SetName("username").SetUnique(true)

	indexes, err := qs.IndexView().CreateOne(index)
	t.Log(indexes, err)

}
func TestQsIndexDropOne(t *testing.T) {
	o := NewOrm()
	o.Using("default")

	qs := o.QueryTable("log")
	err := qs.IndexView().DropOne("username")
	t.Log(err)
}
