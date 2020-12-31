package orm

import (
	"testing"
	"time"

	"github.com/souliot/siot-orm/orm"

	_ "github.com/ClickHouse/clickhouse-go"
)

var (
	clickAddress = "tcp://192.168.0.4:9008?username=default&password=watrix888"
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
func (m *Logs) TablePk() string {
	return "_time"
}

func init() {
	orm.ResetModelCache()
	orm.RegisterModel(new(Logs))
	// RegisterDriver("mongo", DRMongo)
	// RegisterDataBase("default", "mongo", "mongodb://yapi:abcd1234@192.168.50.200:27017/yapi", true)
	orm.ReleaseDataBase("default")
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

func TestRaw(t *testing.T) {
	o := orm.NewOrm()
	o.Using("default")
	var res orm.ParamsList
	_, err := o.Raw("SELECT count() as cnt FROM darwin_log").ValuesFlat(&res)

	t.Log(res, err)
}

func TestCreateDB(t *testing.T) {
	o := orm.NewOrm()
	o.Using("default")
	_, err := o.Raw(`
	CREATE DATABASE IF NOT EXISTS test
	`).Exec()
	t.Log(err)
}
func TestCreateTable(t *testing.T) {
	o := orm.NewOrm()
	o.Using("default")
	_, err := o.Raw(`
	CREATE TABLE test.darwin_log_1 (
		Time DateTime,
		Date Date DEFAULT toDate(Time),
		Level String,
		FuncCall String,
		ServiceName String,
		Address String,
		Message String
	) ENGINE = MergeTree() PARTITION BY toYYYYMM(Date) PRIMARY KEY (Time, ServiceName, Level)
	ORDER BY
		(Time, ServiceName, Level) SETTINGS index_granularity = 8192
	`).Exec()
	t.Log(err)
}
