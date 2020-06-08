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
	"reflect"
	"time"
)

// Fielder define field info
type Fielder interface {
	String() string
	FieldType() int
	SetRaw(interface{}) error
	RawValue() interface{}
}

// Ormer define the orm interface
type Ormer interface {
	Read(interface{}, ...string) error
	ReadOrCreate(interface{}, string, ...string) (bool, interface{}, error)
	Insert(interface{}) (interface{}, error)
	InsertMulti(int, interface{}) (interface{}, error)
	// InsertOrUpdate(md interface{}, colConflitAndArgs ...string) (int64, error)
	Update(interface{}, ...string) (interface{}, error)
	Delete(interface{}, ...string) (interface{}, error)

	QueryTable(interface{}) QuerySeter

	Begin() error
	Commit() error
	Rollback() error
	Using(string) error

	Raw(query string, args ...interface{}) RawSeter
}

type QuerySeter interface {
	Filter(string, ...interface{}) QuerySeter
	FilterRaw(string, string) QuerySeter
	Exclude(string, ...interface{}) QuerySeter
	SetCond(*Condition) QuerySeter
	GetCond() *Condition
	Limit(limit interface{}, args ...interface{}) QuerySeter
	Offset(interface{}) QuerySeter
	GroupBy(...string) QuerySeter
	OrderBy(...string) QuerySeter
	RelatedSel(...interface{}) QuerySeter
	ForUpdate() QuerySeter
	Count() (int64, error)
	Exist() bool
	Update(OperatorUpdate, Params) (int64, error)
	Delete() (int64, error)
	All(interface{}, ...string) error
	One(interface{}, ...string) error
	Distinct(string) ([]interface{}, error)
	Values(*[]Params, ...string) (int64, error)
	ValuesList(*[]ParamsList, ...string) (int64, error)
	ValuesFlat(*ParamsList, string) (int64, error)
	RowsToMap(result *Params, keyCol, valueCol string) (int64, error)
	RowsToStruct(ptrStruct interface{}, keyCol, valueCol string) (int64, error)

	IndexView() IndexViewer
}

type IndexViewer interface {
	List() (interface{}, error)
	CreateOne(Index, ...time.Duration) (string, error)
	CreateMany([]Index, ...time.Duration) ([]string, error)

	DropOne(string, ...time.Duration) error
	DropAll(...time.Duration) error
}

type dbQuerier interface {
	Begin() error
	Commit() error
	Rollback() error

	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// base database struct
type dbBaser interface {
	Read(dbQuerier, *modelInfo, reflect.Value, interface{}, *time.Location, []string, bool) error
	InsertOne(dbQuerier, *modelInfo, reflect.Value, interface{}, *time.Location) (interface{}, error)
	InsertMulti(dbQuerier, *modelInfo, reflect.Value, int, interface{}, *time.Location) (interface{}, error)
	Update(dbQuerier, *modelInfo, reflect.Value, *time.Location, []string) (interface{}, error)
	Delete(dbQuerier, *modelInfo, reflect.Value, *time.Location, []string) (interface{}, error)

	FindOne(dbQuerier, *querySet, *modelInfo, *Condition, interface{}, *time.Location, []string) error
	Distinct(dbQuerier, *querySet, *modelInfo, *Condition, *time.Location, string) ([]interface{}, error)
	ReadBatch(dbQuerier, *querySet, *modelInfo, *Condition, interface{}, *time.Location, []string) error
	Count(dbQuerier, *querySet, *modelInfo, *Condition, *time.Location) (int64, error)
	UpdateBatch(dbQuerier, *querySet, *modelInfo, *Condition, OperatorUpdate, Params, *time.Location) (int64, error)
	DeleteBatch(dbQuerier, *querySet, *modelInfo, *Condition, *time.Location) (int64, error)
	Indexes(*querySet, *modelInfo, *time.Location) IndexViewer
	TimeFromDB(*time.Time, *time.Location)
	TimeToDB(*time.Time, *time.Location)

	SupportUpdateJoin() bool
	MaxLimit() uint64
	TableQuote() string
	ReplaceMarks(*string)
	OperatorSQL(string) string
	GenerateOperatorSQL(*modelInfo, *fieldInfo, string, []interface{}, *time.Location) (string, []interface{})
	GenerateOperatorLeftCol(*fieldInfo, string, *string)
}

// RawPreparer raw query statement
type RawPreparer interface {
	Exec(...interface{}) (sql.Result, error)
	Close() error
}

// RawSeter raw query seter
// create From Ormer.Raw
// for example:
//  sql := fmt.Sprintf("SELECT %sid%s,%sname%s FROM %suser%s WHERE id = ?",Q,Q,Q,Q,Q,Q)
//  rs := Ormer.Raw(sql, 1)
type RawSeter interface {
	//execute sql and get result
	Exec() (sql.Result, error)
	//query data and map to container
	//for example:
	//	var name string
	//	var id int
	//	rs.QueryRow(&id,&name) // id==2 name=="slene"
	QueryRow(containers ...interface{}) error

	// query data rows and map to container
	//	var ids []int
	//	var names []int
	//	query = fmt.Sprintf("SELECT 'id','name' FROM %suser%s", Q, Q)
	//	num, err = dORM.Raw(query).QueryRows(&ids,&names) // ids=>{1,2},names=>{"nobody","slene"}
	QueryRows(containers ...interface{}) (int64, error)
	SetArgs(...interface{}) RawSeter
	// query data to []map[string]interface
	// see QuerySeter's Values
	Values(container *[]Params, cols ...string) (int64, error)
	// query data to [][]interface
	// see QuerySeter's ValuesList
	ValuesList(container *[]ParamsList, cols ...string) (int64, error)
	// query data to []interface
	// see QuerySeter's ValuesFlat
	ValuesFlat(container *ParamsList, cols ...string) (int64, error)
	// query all rows into map[string]interface with specify key and value column name.
	// keyCol = "name", valueCol = "value"
	// table data
	// name  | value
	// total | 100
	// found | 200
	// to map[string]interface{}{
	// 	"total": 100,
	// 	"found": 200,
	// }
	RowsToMap(result *Params, keyCol, valueCol string) (int64, error)
	// query all rows into struct with specify key and value column name.
	// keyCol = "name", valueCol = "value"
	// table data
	// name  | value
	// total | 100
	// found | 200
	// to struct {
	// 	Total int
	// 	Found int
	// }
	RowsToStruct(ptrStruct interface{}, keyCol, valueCol string) (int64, error)

	// return prepared raw statement for used in times.
	// for example:
	// 	pre, err := dORM.Raw("INSERT INTO tag (name) VALUES (?)").Prepare()
	// 	r, err := pre.Exec("name1") // INSERT INTO tag (name) VALUES (`name1`)
	Prepare() (RawPreparer, error)
}

// stmtQuerier statement querier
type stmtQuerier interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	//ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	//QueryContext(args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
	//QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row
}
