package pool

import (
	"time"
)

type DriverType int

// Enum the Database driver
// const (
// 	_            DriverType = iota // int enum type
// 	DRMongo                        // MongoDB
// 	DRClickHouse                   // ClickHouse
// )

// var (
// 	driversName = map[DriverType]string{
// 		DRMongo:      "mongo",
// 		DRClickHouse: "clickhouse",
// 	}
// )

type Pooler interface {
	Factory() (interface{}, error)
	Ping(interface{}) error
	Close(interface{}) error
}

func RegisterPool(poolName string, p Pooler, force bool, params ...int) (err error) {
	//factory 创建连接的方法
	factory := func() (interface{}, error) {
		return p.Factory()
	}

	//close 关闭连接的方法
	close := func(v interface{}) error {
		return p.Close(v)
	}

	//ping 检测连接的方法
	ping := func(v interface{}) error {
		return p.Ping(v)
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
	pool, err := NewChannelPool(poolConfig)
	if err != nil {
		return ErrRegisterPool
	}

	if !pools.add(poolName, pool, force) {
		return ErrRegisterPool
	}
	return
}

func GetClient(poolName string) (c interface{}, err error) {
	if p, ok := pools.get(poolName); ok {
		v, err := p.Get()
		if err == nil {
			c = v
			defer PutClient(poolName, c)
		}
		return c, err
	}
	return nil, ErrGetConnection
}

func PutClient(poolName string, c interface{}) (err error) {
	if p, ok := pools.get(poolName); ok {
		err = p.Put(c)
		return
	}
	return ErrPutConnection
}

func ReleasePool(poolName string) {
	if p, ok := pools.get(poolName); ok {
		p.Release()
		pools.del(poolName)
		return
	}
	return
}

func IsRegister(poolName string) (ok bool) {
	_, ok = pools.get(poolName)
	return
}

func ReleaseAll() {
	for k, v := range pools.cache {
		v.Release()
		pools.del(k)
	}
}
