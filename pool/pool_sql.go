package pool

import (
	"context"
	"database/sql"
	"time"
)

type DriverType int

// Enum the Database driver
const (
	_            DriverType = iota // int enum type
	DRMySQL                        // mysql
	DRSqlite                       // sqlite
	DROracle                       // oracle
	DRPostgres                     // pgsql
	DRTiDB                         // TiDB
	DRMongo                        // MongoDB
	DRClickHouse                   // ClickHouse
)

var (
	driversName = map[DriverType]string{
		DRMySQL:      "mysql",
		DRSqlite:     "sqlite3",
		DROracle:     "oracle",
		DRPostgres:   "postgres",
		DRTiDB:       "tidb",
		DRMongo:      "mongo",
		DRClickHouse: "clickhouse",
	}
)

func RegisterSqlPool(poolName string, t int, url string, force bool, params ...int) (err error) {
	//factory 创建连接的方法
	factory := func() (interface{}, error) {
		return sql.Open(driversName[DriverType(t)], url)
	}

	//close 关闭连接的方法
	close := func(v interface{}) error {
		return v.(*sql.DB).Close()
	}

	//ping 检测连接的方法
	ping := func(v interface{}) error {
		return v.(*sql.DB).PingContext(context.Background())
	}

	var (
		size int           = 5
		cap  int           = 20
		idle time.Duration = 30
	)

	for i, v := range params {
		switch i {
		case 0:
			size = v
		case 1:
			cap = v
		case 2:
			idle = time.Duration(v)
		}
	}

	//创建一个连接池： 初始化5，最大连接30
	poolConfig := &Config{
		InitialCap: size,
		MaxCap:     cap,
		Factory:    factory,
		Close:      close,
		Ping:       ping,
		//连接最大空闲时间，超过该时间的连接 将会关闭，可避免空闲时连接EOF，自动失效的问题
		IdleTimeout: idle * time.Second,
	}
	mgoPool, err := NewChannelPool(poolConfig)

	if !pools.add(poolName, mgoPool, force) {
		return ErrRegisterPool
	}
	return
}
