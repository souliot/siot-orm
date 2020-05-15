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
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

// Log implement the log.Logger
type Log struct {
	*log.Logger
}

//costomer log func
var LogFunc func(query map[string]interface{})

// NewLog set io.Writer to create a Logger.
func NewLog(out io.Writer) *Log {
	d := new(Log)
	d.Logger = log.New(out, "[ORM]", log.LstdFlags)
	return d
}

func debugLogQueies(alias *alias, operaton, query string, t time.Time, err error, args ...interface{}) {
	var logMap = make(map[string]interface{})
	sub := time.Now().Sub(t) / 1e5
	elsp := float64(int(sub)) / 10.0
	logMap["cost_time"] = elsp
	flag := "  OK"
	if err != nil {
		flag = "FAIL"
	}
	logMap["flag"] = flag
	con := fmt.Sprintf(" -[Queries/%s] - [%s / %11s / %7.1fms] - [%s]", alias.Name, flag, operaton, elsp, query)
	cons := make([]string, 0, len(args))
	for _, arg := range args {
		cons = append(cons, fmt.Sprintf("%v", arg))
	}
	if len(cons) > 0 {
		con += fmt.Sprintf(" - `%s`", strings.Join(cons, "`, `"))
	}
	if err != nil {
		con += " - " + err.Error()
	}
	logMap["sql"] = fmt.Sprintf("%s-`%s`", query, strings.Join(cons, "`, `"))
	if LogFunc != nil {
		LogFunc(logMap)
	}
	DebugLog.Println(con)
}

// statement query logger struct.
// if dev mode, use stmtQueryLog, or use stmtQuerier.
type stmtQueryLog struct {
	alias *alias
	query string
	stmt  stmtQuerier
}

var _ stmtQuerier = new(stmtQueryLog)

func (d *stmtQueryLog) Close() error {
	a := time.Now()
	err := d.stmt.Close()
	debugLogQueies(d.alias, "st.Close", d.query, a, err)
	return err
}

func (d *stmtQueryLog) Exec(args ...interface{}) (sql.Result, error) {
	a := time.Now()
	res, err := d.stmt.Exec(args...)
	debugLogQueies(d.alias, "st.Exec", d.query, a, err, args...)
	return res, err
}

func (d *stmtQueryLog) Query(args ...interface{}) (*sql.Rows, error) {
	a := time.Now()
	res, err := d.stmt.Query(args...)
	debugLogQueies(d.alias, "st.Query", d.query, a, err, args...)
	return res, err
}

func (d *stmtQueryLog) QueryRow(args ...interface{}) *sql.Row {
	a := time.Now()
	res := d.stmt.QueryRow(args...)
	debugLogQueies(d.alias, "st.QueryRow", d.query, a, nil, args...)
	return res
}
func newStmtQueryLog(alias *alias, stmt stmtQuerier, query string) stmtQuerier {
	d := new(stmtQueryLog)
	d.stmt = stmt
	d.alias = alias
	d.query = query
	return d
}
