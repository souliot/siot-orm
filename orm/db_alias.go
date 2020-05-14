// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/souliot/siot-orm/pool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// DriverType database driver constant int.
type DriverType int

// Enum the Database driver
const (
	_            DriverType = iota // int enum type
	DRMongo                        // MongoDB
	DRClickHouse                   // ClickHouse
)

var (
	ErrNoDriver = errors.New("db driver has not registered")
)

var (
	dataBaseCache = &_dbCache{cache: make(map[string]*alias)}
	drivers       = map[string]DriverType{
		// "mongo":      DRMongo,
		// "clickhouse": DRClickHouse,
	}
	dbBasers = map[DriverType]dbBaser{
		DRMongo:      newdbBaseMongo(),
		DRClickHouse: newdbBaseClickHouse(),
	}
)

// database alias cacher.
type _dbCache struct {
	mux   sync.RWMutex
	cache map[string]*alias
}

// add database alias with original name.
func (ac *_dbCache) add(name string, al *alias, force bool) (added bool) {
	ac.mux.Lock()
	defer ac.mux.Unlock()

	if force {
		ac.cache[name] = al
		added = true
		return
	}

	if _, ok := ac.cache[name]; !ok {
		ac.cache[name] = al
		added = true
	}
	return
}

// get database alias if cached.
func (ac *_dbCache) get(name string) (al *alias, ok bool) {
	ac.mux.RLock()
	defer ac.mux.RUnlock()
	al, ok = ac.cache[name]
	return
}

// delete database alias if cached.
func (ac *_dbCache) del(name string) {
	ac.mux.RLock()
	defer ac.mux.RUnlock()
	if _, ok := ac.cache[name]; ok {
		delete(ac.cache, name)
	}
	return
}

// get default alias.
func (ac *_dbCache) getDefault() (al *alias) {
	al, _ = ac.get("default")
	return
}

type DB struct {
	*sync.RWMutex
	DbType  DriverType
	MDB     *mongo.Database
	Session mongo.Session
	DB      *sql.DB
	TX      interface{}
	isTx    bool
	stmts   map[string]*sql.Stmt
}

var _ dbQuerier = new(DB)

func (d *DB) Begin() (err error) {
	d.isTx = true
	switch d.DbType {
	case DRMongo:
		d.Session, err = d.MDB.Client().StartSession()
		if err != nil {
			return
		}
		defer d.Session.EndSession(todo)

		//开始事务
		err = d.Session.StartTransaction()
	case DRClickHouse:
		d.TX, err = d.DB.Begin()
	default:
	}

	return
}

func (d *DB) Commit() (err error) {
	d.isTx = false
	switch d.DbType {
	case DRMongo:
		return d.Session.CommitTransaction(todo)
	case DRClickHouse:
		return d.TX.(*sql.Tx).Commit()
	default:
	}
	return
}
func (d *DB) Rollback() (err error) {
	d.isTx = false
	switch d.DbType {
	case DRMongo:
		return d.Session.AbortTransaction(todo)
	case DRClickHouse:
		return d.TX.(*sql.Tx).Rollback()
	default:
	}
	return
}
func (d *DB) getStmt(query string) (*sql.Stmt, error) {
	d.RLock()
	if stmt, ok := d.stmts[query]; ok {
		d.RUnlock()
		return stmt, nil
	}
	d.RUnlock()

	stmt, err := d.Prepare(query)
	if err != nil {
		return nil, err
	}
	d.Lock()
	d.stmts[query] = stmt
	d.Unlock()
	return stmt, nil
}

func (d *DB) Prepare(query string) (st *sql.Stmt, err error) {
	switch d.DbType {
	case DRMongo:
		return nil, nil
	case DRClickHouse:
		if d.isTx {
			return d.TX.(*sql.Tx).Prepare(query)
		} else {
			return d.DB.Prepare(query)
		}
	default:
	}
	return
}

func (d *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.DB.PrepareContext(ctx, query)
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := d.getStmt(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := d.getStmt(query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := d.getStmt(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := d.getStmt(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	stmt, err := d.getStmt(query)
	if err != nil {
		panic(err)
	}
	return stmt.QueryRow(args...)

}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {

	stmt, err := d.getStmt(query)
	if err != nil {
		panic(err)
	}
	return stmt.QueryRowContext(ctx, args)
}

type alias struct {
	Name         string
	Driver       DriverType
	DriverName   string
	DataSource   string
	DbName       string
	MaxIdleConns int
	MaxOpenConns int
	DB           *DB
	DbBaser      dbBaser
	TZ           *time.Location
	Engine       string
}

func (al *alias) getDB() (db *DB, err error) {
	if al.Name == "" {
		al.Name = "default"
	}
	client, err := pool.GetClient(al.Name)
	if err != nil {
		DebugLog.Println(err.Error())
		return
	}

	if al.Driver == DRMongo {
		db = &DB{
			DbType:  DRMongo,
			MDB:     client.(*mongo.Client).Database(al.DbName),
			Session: nil,
			RWMutex: new(sync.RWMutex),
		}
	} else {
		db = &DB{
			DbType:  DRClickHouse,
			DB:      client.(*sql.DB),
			stmts:   make(map[string]*sql.Stmt),
			RWMutex: new(sync.RWMutex),
		}
	}
	return
}

func detectTZ(al *alias) {
	// orm timezone system match database
	// default use Local
	al.TZ = DefaultTimeLoc
}

func addAlias(aliasName, driverName string, force bool) (*alias, error) {
	al := new(alias)
	al.Name = aliasName
	al.DriverName = driverName

	if dr, ok := drivers[driverName]; ok {
		al.DbBaser = dbBasers[dr]
		al.Driver = dr
	} else {
		return nil, fmt.Errorf("driver name `%s` have not registered", driverName)
	}

	if !dataBaseCache.add(aliasName, al, force) {
		return nil, fmt.Errorf("DataBase alias name `%s` already registered, cannot reuse", aliasName)
	}

	return al, nil
}

// AddAliasWthDB add a aliasName for the drivename
func AddAlias(aliasName, driverName string) error {
	_, err := addAlias(aliasName, driverName, false)
	return err
}

// RegisterDataBase Setting the database connect params. Use the database driver self dataSource args.
func RegisterDataBase(aliasName, driverName, dataSource string, force bool, params ...int) (err error) {
	var (
		al *alias
	)
	if t, ok := drivers[driverName]; ok {
		switch t {
		case DRMongo:
			err = pool.RegisterMgoPool(aliasName, dataSource, force, params...)
		case DRClickHouse:
			err = pool.RegisterClickPool(aliasName, dataSource, force, params...)
		default:
			err = ErrNoDriver
		}
	}
	if err != nil {
		DebugLog.Println(err.Error())
		return
	}

	al, err = addAlias(aliasName, driverName, force)
	if err != nil {
		DebugLog.Println(err.Error())
		return
	}

	al.DataSource = dataSource
	al.DbName = getDatabase(al.DataSource)

	detectTZ(al)

	return
}

func ReleaseDataBase(aliasName string) {
	dataBaseCache.del(aliasName)
	pool.ReleasePool(aliasName)
}

// RegisterDriver Register a database driver use specify driver name, this can be definition the driver is which database type.
func RegisterDriver(driverName string, typ DriverType) error {
	if t, ok := drivers[driverName]; !ok {
		drivers[driverName] = typ
	} else {
		if t != typ {
			return fmt.Errorf("driverName `%s` db driver already registered and is other type", driverName)
		}
	}
	return nil
}

// SetDataBaseTZ Change the database default used timezone
func SetDataBaseTZ(aliasName string, tz *time.Location) error {
	if al, ok := dataBaseCache.get(aliasName); ok {
		al.TZ = tz
	} else {
		return fmt.Errorf("DataBase alias name `%s` not registered", aliasName)
	}
	return nil
}

func getDatabase(uri string) (dbName string) {
	cs, err := connstring.Parse(uri)
	if err != nil {
		dbName = "test"
		return
	}
	dbName = cs.Database
	return
}
