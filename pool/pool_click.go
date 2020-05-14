package pool

import (
	"context"
	"database/sql"
)

type ClickConn struct {
	Address string
}

func (c *ClickConn) Factory() (v interface{}, err error) {
	return sql.Open("clickhouse", c.Address)
}
func (c *ClickConn) Ping(v interface{}) (err error) {
	return v.(*sql.DB).PingContext(context.Background())
}
func (c *ClickConn) Close(v interface{}) (err error) {
	return v.(*sql.DB).Close()
}

func RegisterClickPool(poolName string, url string, force bool, params ...int) (err error) {
	return RegisterPool(poolName, &ClickConn{Address: url}, force, params...)
}
