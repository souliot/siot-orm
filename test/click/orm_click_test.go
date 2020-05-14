package orm

import (
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/souliot/siot-orm/orm"
)

type Logs struct {
	Time        time.Time `orm:"pk;column(_time);type(datetime)" bson:"time"`
	Date        time.Time `orm:"column(_date);null" bson:"date"`
	Level       string    `orm:"column(level)"  bson:"level"`
	FuncCall    string    `orm:"column(func_call)"  bson:"func_call"`
	ServiceName string    `orm:"column(service_name)"  bson:"service_name"`
	Address     string    `orm:"column(address)"  bson:"address"`
	Message     string    `orm:"column(message)"  bson:"message"`
}

func (m *Logs) TableName() string {
	return "darwin_log"
}

func init() {
	orm.RegisterModel(new(Logs))

	// RegisterDriver("mongo", DRMongo)
	// RegisterDataBase("default", "mongo", "mongodb://yapi:abcd1234@192.168.50.200:27017/yapi", true)
	orm.RegisterDriver("clickhouse", orm.DRClickHouse)
	orm.RegisterDataBase("default", "clickhouse", "tcp://192.168.0.8:9000?username=default&password=watrix888&database=log&read_timeout=10&write_timeout=20", true)
	// RegisterDataBase("default", "mysql", "root:watrix@/businessserver?charset=utf8", true)
	// RegisterDataBase("default", "mongo", "mongodb://yapi:abcd1234@vm:27017/yapi")

}

var (
	l = Logs{
		// Time:        "asd",
		// Date:        "linleizhou1234",
		Level: "[I]",
		// ServiceName: "linleizhou1234",
		// Address:     "group",
		// Message:     "group",
	}
)

func TestRead(t *testing.T) {
	o := orm.NewOrm()
	o.Using("default")

	err := o.Read(&l, "Level")
	t.Log(err, l)
}
func TestInsert(t *testing.T) {
	o := orm.NewOrm()
	o.Using("default")

	ls := []Logs{}
	l := Logs{
		Time:        time.Now(),
		Level:       "[E]",
		ServiceName: "test",
		FuncCall:    "[test.go:21]",
		Address:     "192.168.0.148:8880",
		Message:     "test message error",
	}
	for i := 0; i < 100; i++ {
		ls = append(ls, l)
	}
	cnt, err := o.InsertMulti(1000, ls)
	t.Log(err, cnt)
}

type Stu struct {
	Str  string
	Time time.Time
}
