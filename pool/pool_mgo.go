package pool

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MgoConn struct {
	Address string
}

func (c *MgoConn) Factory() (v interface{}, err error) {
	return mongo.Connect(context.Background(), options.Client().ApplyURI(c.Address))
}
func (c *MgoConn) Ping(v interface{}) (err error) {
	return v.(*mongo.Client).Ping(context.Background(), readpref.Primary())
}
func (c *MgoConn) Close(v interface{}) (err error) {
	return v.(*mongo.Client).Disconnect(context.Background())
}

func RegisterMgoPool(poolName string, url string, force bool, params ...int) (err error) {
	return RegisterPool(poolName, &MgoConn{Address: url}, force, params...)
}
