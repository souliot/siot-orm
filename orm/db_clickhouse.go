package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

var (
	OpDefault OperatorUpdate = "$set"
)
var clickOperators = map[string]string{
	"exact":     "= ?",
	"iexact":    "LIKE ?",
	"contains":  "LIKE BINARY ?",
	"icontains": "LIKE ?",
	// "regex":       "REGEXP BINARY ?",
	// "iregex":      "REGEXP ?",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"eq":          "= ?",
	"ne":          "!= ?",
	"startswith":  "LIKE BINARY ?",
	"endswith":    "LIKE BINARY ?",
	"istartswith": "LIKE ?",
	"iendswith":   "LIKE ?",
}

// mysql dbBaser implementation.
type dbBaseClickHouse struct {
	dbBase
}

var _ dbBaser = new(dbBaseClickHouse)

// create new mysql dbBaser.
func newdbBaseClickHouse() dbBaser {
	b := new(dbBaseClickHouse)
	b.ins = b
	return b
}

// get mysql operator.
func (d *dbBaseClickHouse) OperatorSQL(operator string) string {
	return clickOperators[operator]
}

// read one record.
func (d *dbBaseClickHouse) FindOne(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
	qs.limit = 1
	err = d.ReadBatch(q, qs, mi, cond, container, tz, cols)
	if err != nil {
		return err
	}

	return nil
}

// read one record.
func (d *dbBaseClickHouse) Distinct(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location, field string) (res []interface{}, err error) {
	return
}

// read all records.
func (d *dbBaseClickHouse) ReadBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
	val := reflect.ValueOf(container)
	ind := reflect.Indirect(val)

	errTyp := true
	one := true
	isPtr := true

	if val.Kind() == reflect.Ptr {
		fn := ""
		if ind.Kind() == reflect.Slice {
			one = false
			typ := ind.Type().Elem()
			switch typ.Kind() {
			case reflect.Ptr:
				fn = getFullName(typ.Elem())
			case reflect.Struct:
				isPtr = false
				fn = getFullName(typ)
			}
		} else {
			fn = getFullName(ind.Type())
		}
		errTyp = fn != mi.fullName
	}

	if errTyp {
		if one {
			panic(fmt.Errorf("wrong object type `%s` for rows scan, need *%s", val.Type(), mi.fullName))
		} else {
			panic(fmt.Errorf("wrong object type `%s` for rows scan, need *[]*%s or *[]%s", val.Type(), mi.fullName, mi.fullName))
		}
	}

	rlimit := qs.limit
	offset := qs.offset

	Q := d.ins.TableQuote()

	var tCols []string
	if len(cols) > 0 {
		hasRel := len(qs.related) > 0 || qs.relDepth > 0
		tCols = make([]string, 0, len(cols))
		var maps map[string]bool
		if hasRel {
			maps = make(map[string]bool)
		}
		for _, col := range cols {
			if fi, ok := mi.fields.GetByAny(col); ok {
				tCols = append(tCols, fi.column)
				if hasRel {
					maps[fi.column] = true
				}
			} else {
				return fmt.Errorf("wrong field/column name `%s`", col)
			}
		}
		if hasRel {
			for _, fi := range mi.fields.fieldsDB {
				if fi.fieldType&IsRelField > 0 {
					if !maps[fi.column] {
						tCols = append(tCols, fi.column)
					}
				}
			}
		}
	} else {
		tCols = mi.fields.dbcols
	}

	colsNum := len(tCols)
	sep := fmt.Sprintf("%s, T0.%s", Q, Q)
	sels := fmt.Sprintf("T0.%s%s%s", Q, strings.Join(tCols, sep), Q)

	tables := newDbTables(mi, d.ins)
	tables.parseRelated(qs.related, qs.relDepth)

	where, args := tables.getCondSQL(cond, false, tz)
	groupBy := tables.getGroupSQL(qs.groups)
	orderBy := tables.getOrderSQL(qs.orders)
	limit := tables.getLimitSQL(mi, offset, rlimit)
	join := tables.getJoinSQL()

	for _, tbl := range tables.tables {
		if tbl.sel {
			colsNum += len(tbl.mi.fields.dbcols)
			sep := fmt.Sprintf("%s, %s.%s", Q, tbl.index, Q)
			sels += fmt.Sprintf(", %s.%s%s%s", tbl.index, Q, strings.Join(tbl.mi.fields.dbcols, sep), Q)
		}
	}

	sqlSelect := "SELECT"
	if qs.distinct {
		sqlSelect += " DISTINCT"
	}
	query := fmt.Sprintf("%s %s FROM %s%s%s T0 %s%s%s%s%s", sqlSelect, sels, Q, mi.table, Q, join, where, groupBy, orderBy, limit)

	if qs.forupdate {
		query += " FOR UPDATE"
	}

	d.ins.ReplaceMarks(&query)

	var rs *sql.Rows
	if qs != nil && qs.forContext {
		rs, err = q.QueryContext(qs.ctx, query, args...)
		if err != nil {
			return err
		}
	} else {
		rs, err = q.Query(query, args...)
		if err != nil {
			return err
		}
	}

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	defer rs.Close()

	slice := ind

	var cnt int64
	for rs.Next() {
		if one && cnt == 0 || !one {
			if err := rs.Scan(refs...); err != nil {
				return err
			}

			elm := reflect.New(mi.addrField.Elem().Type())
			mind := reflect.Indirect(elm)

			cacheV := make(map[string]*reflect.Value)
			cacheM := make(map[string]*modelInfo)
			trefs := refs

			d.setColsValues(mi, &mind, tCols, refs[:len(tCols)], tz)
			trefs = refs[len(tCols):]

			for _, tbl := range tables.tables {
				// loop selected tables
				if tbl.sel {
					last := mind
					names := ""
					mmi := mi
					// loop cascade models
					for _, name := range tbl.names {
						names += name
						if val, ok := cacheV[names]; ok {
							last = *val
							mmi = cacheM[names]
						} else {
							fi := mmi.fields.GetByName(name)
							lastm := mmi
							mmi = fi.relModelInfo
							field := last
							if last.Kind() != reflect.Invalid {
								field = reflect.Indirect(last.FieldByIndex(fi.fieldIndex))
								if field.IsValid() {
									d.setColsValues(mmi, &field, mmi.fields.dbcols, trefs[:len(mmi.fields.dbcols)], tz)
									for _, fi := range mmi.fields.fieldsReverse {
										if fi.inModel && fi.reverseFieldInfo.mi == lastm {
											if fi.reverseFieldInfo != nil {
												f := field.FieldByIndex(fi.fieldIndex)
												if f.Kind() == reflect.Ptr {
													f.Set(last.Addr())
												}
											}
										}
									}
									last = field
								}
							}
							cacheV[names] = &field
							cacheM[names] = mmi
						}
					}
					trefs = trefs[len(mmi.fields.dbcols):]
				}
			}

			if one {
				ind.Set(mind)
			} else {
				if cnt == 0 {
					// you can use a empty & caped container list
					// orm will not replace it
					if ind.Len() != 0 {
						// if container is not empty
						// create a new one
						slice = reflect.New(ind.Type()).Elem()
					}
				}

				if isPtr {
					slice = reflect.Append(slice, mind.Addr())
				} else {
					slice = reflect.Append(slice, mind)
				}
			}
		}
		cnt++
	}

	if !one {
		if cnt > 0 {
			ind.Set(slice)
		} else {
			// when a result is empty and container is nil
			// to set a empty container
			if ind.IsNil() {
				ind.Set(reflect.MakeSlice(ind.Type(), 0, 0))
			}
		}
	}

	return nil
}

// get the recodes count.
func (d *dbBaseClickHouse) Count(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (cnt int64, err error) {
	tables := newDbTables(mi, d.ins)
	tables.parseRelated(qs.related, qs.relDepth)

	where, args := tables.getCondSQL(cond, false, tz)
	groupBy := tables.getGroupSQL(qs.groups)
	tables.getOrderSQL(qs.orders)
	join := tables.getJoinSQL()

	Q := d.ins.TableQuote()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s%s T0 %s%s%s", Q, mi.table, Q, join, where, groupBy)

	if groupBy != "" {
		query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS T", query)
	}

	d.ins.ReplaceMarks(&query)

	var row *sql.Row
	if qs != nil && qs.forContext {
		row = q.QueryRowContext(qs.ctx, query, args...)
	} else {
		row = q.QueryRow(query, args...)
	}
	err = row.Scan(&cnt)
	return
}

// update the recodes.
func (d *dbBaseClickHouse) UpdateBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, operator OperatorUpdate, params Params, tz *time.Location) (i int64, err error) {
	columns := make([]string, 0, len(params))
	values := make([]interface{}, 0, len(params))
	for col, val := range params {
		if fi, ok := mi.fields.GetByAny(col); !ok || !fi.dbcol {
			panic(fmt.Errorf("wrong field/column name `%s`", col))
		} else {
			columns = append(columns, fi.column)
			values = append(values, val)
		}
	}

	if len(columns) == 0 {
		panic(fmt.Errorf("update params cannot empty"))
	}

	tables := newDbTables(mi, d.ins)
	if qs != nil {
		tables.parseRelated(qs.related, qs.relDepth)
	}

	where, args := tables.getCondSQL(cond, false, tz)

	values = append(values, args...)

	join := tables.getJoinSQL()

	var query string

	Q := d.ins.TableQuote()

	cols := make([]string, 0, len(columns))

	for i, v := range columns {
		col := fmt.Sprintf("%s%s%s", Q, v, Q)
		if c, ok := values[i].(colValue); ok {
			switch c.opt {
			case ColAdd:
				cols = append(cols, col+" = "+col+" + ?")
			case ColMinus:
				cols = append(cols, col+" = "+col+" - ?")
			case ColMultiply:
				cols = append(cols, col+" = "+col+" * ?")
			case ColExcept:
				cols = append(cols, col+" = "+col+" / ?")
			}
			values[i] = c.value
		} else {
			cols = append(cols, col+" = ?")
		}
	}

	sets := strings.Join(cols, ", ") + " "

	where = strings.ReplaceAll(where, "T0.", "")

	query = fmt.Sprintf("ALTER TABLE %s%s%s UPDATE %s %s%s", Q, mi.table, Q, join, sets, where)

	d.ins.ReplaceMarks(&query)

	i, err = d.Count(q, qs, mi, cond, tz)
	if err != nil {
		return 0, err
	}

	if qs != nil && qs.forContext {
		_, err = q.ExecContext(qs.ctx, query, values...)
	} else {
		_, err = q.Exec(query, values...)
	}
	return
}

// delete the recodes.
func (d *dbBaseClickHouse) DeleteBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (i int64, err error) {
	tables := newDbTables(mi, d.ins)
	tables.skipEnd = true

	if qs != nil {
		tables.parseRelated(qs.related, qs.relDepth)
	}

	if cond == nil || cond.IsEmpty() {
		panic(fmt.Errorf("delete operation cannot execute without condition"))
	}

	Q := d.ins.TableQuote()

	where, args := tables.getCondSQL(cond, false, tz)
	where = strings.ReplaceAll(where, "T0.", "")

	query := fmt.Sprintf("ALTER TABLE %s%s%s DELETE %s", Q, mi.table, Q, where)

	d.ins.ReplaceMarks(&query)
	if qs != nil && qs.forContext {
		_, err = q.ExecContext(qs.ctx, query, args...)
	} else {
		_, err = q.Exec(query, args...)
	}
	if err != nil {
		return
	}

	num, err := d.Count(q, qs, mi, cond, tz)
	if err != nil {
		return 0, err
	}
	if num > 0 {
		err := d.deleteRels(q, mi, args, tz)
		if err != nil {
			return num, err
		}
	}
	return num, nil
}

// read one record.
func (d *dbBaseClickHouse) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location, cols []string, isForUpdate bool) (err error) {
	var whereCols []string
	var args []interface{}

	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()
	sep := fmt.Sprintf("%s, %s", Q, Q)
	sels := strings.Join(mi.fields.dbcols, sep)
	colsNum := len(mi.fields.dbcols)

	sep = fmt.Sprintf("%s = ? AND %s", Q, Q)
	wheres := strings.Join(whereCols, sep)

	forUpdate := ""
	if isForUpdate {
		forUpdate = "FOR UPDATE"
	}

	query := fmt.Sprintf("SELECT %s%s%s FROM %s%s%s WHERE %s%s%s = ? %s", Q, sels, Q, Q, mi.table, Q, Q, wheres, Q, forUpdate)

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	d.ins.ReplaceMarks(&query)

	row := q.QueryRow(query, args...)
	if err := row.Scan(refs...); err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}
	elm := reflect.New(mi.addrField.Elem().Type())
	mind := reflect.Indirect(elm)
	d.setColsValues(mi, &mind, mi.fields.dbcols, refs, tz)
	ind.Set(mind)
	return nil
}

// insert one record.
func (d *dbBaseClickHouse) InsertOne(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location) (id interface{}, err error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}
	q.Begin()
	id, err = d.InsertValue(q, mi, false, names, values)
	if err != nil {
		q.Rollback()
		return 0, err
	}
	q.Commit()
	if len(autoFields) > 0 {
		err = d.setval(q, mi, autoFields)
	}
	return id, err
}

// insert all records.
func (d *dbBaseClickHouse) InsertMulti(q dbQuerier, mi *modelInfo, sind reflect.Value, bulk int, field interface{}, tz *time.Location) (ids interface{}, err error) {
	var (
		cnt    int64
		nums   int
		values []interface{}
		names  []string
	)
	// typ := reflect.Indirect(mi.addrField).Type()

	cols := make([]string, 0)

	for _, v := range mi.fields.columns {
		if !v.null {
			cols = append(cols, v.column)
		}
	}

	length, autoFields := sind.Len(), make([]string, 0, 1)
	q.Begin()
	for i := 1; i <= length; i++ {

		ind := reflect.Indirect(sind.Index(i - 1))
		// Is this needed ?
		// if !ind.Type().AssignableTo(typ) {
		// 	return cnt, ErrArgs
		// }

		if i == 1 {
			var (
				vus []interface{}
				err error
			)
			vus, autoFields, err = d.collectValues(mi, ind, cols, false, true, &names, tz)
			if err != nil {
				return cnt, err
			}
			values = make([]interface{}, bulk*len(vus))
			nums += copy(values, vus)
		} else {
			vus, _, err := d.collectValues(mi, ind, cols, false, true, nil, tz)
			if err != nil {
				return cnt, err
			}

			if len(vus) != len(names) {
				return cnt, ErrArgs
			}

			nums += copy(values[nums:], vus)
		}

		if i > 1 && i%bulk == 0 || length == i {
			num, err := d.InsertValue(q, mi, true, names, values[:nums])
			if err != nil {
				q.Rollback()
				return cnt, err
			}
			cnt += num
			nums = 0
		}
	}
	q.Commit()
	if len(autoFields) > 0 {
		err = d.setval(q, mi, autoFields)
	}

	return cnt, err
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
func (d *dbBaseClickHouse) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (cnt int64, err error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	for i := range marks {
		marks[i] = "?"
	}

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)
	cnt = int64(multi)

	// if isMulti {
	// 	qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	// }

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)
	d.ReplaceMarks(&query)

	if isMulti || !d.HasReturningID(mi, &query) {
		start := 0
		end := 0
		for i := 0; i < multi; i++ {
			start = i * len(names)
			end = (i + 1) * len(names)
			_, err = q.Exec(query, values[start:end]...)
			if err != nil {
				cnt -= 1
				continue
			}
		}
	}

	return cnt, err
}

// update one record.
func (d *dbBaseClickHouse) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (id interface{}, err error) {
	pkName, pkValue, ok := getExistPk(mi, ind)
	if !ok {
		return 0, ErrMissPK
	}

	var setNames []string

	// if specify cols length is zero, then commit all columns.
	if len(cols) == 0 {
		cols = mi.fields.dbcols
		setNames = make([]string, 0, len(mi.fields.dbcols)-1)
	} else {
		setNames = make([]string, 0, len(cols))
	}

	setValues, _, err := d.collectValues(mi, ind, cols, true, false, &setNames, tz)
	if err != nil {
		return 0, err
	}

	var findAutoNowAdd, findAutoNow bool
	var index int
	for i, col := range setNames {
		if mi.fields.GetByColumn(col).autoNowAdd {
			index = i
			findAutoNowAdd = true
		}
		if mi.fields.GetByColumn(col).autoNow {
			findAutoNow = true
		}
	}
	if findAutoNowAdd {
		setNames = append(setNames[0:index], setNames[index+1:]...)
		setValues = append(setValues[0:index], setValues[index+1:]...)
	}

	if !findAutoNow {
		for col, info := range mi.fields.columns {
			if info.autoNow {
				setNames = append(setNames, col)
				setValues = append(setValues, time.Now())
			}
		}
	}

	setValues = append(setValues, pkValue)

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ?, %s", Q, Q)
	setColumns := strings.Join(setNames, sep)

	query := fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = ? WHERE %s%s%s = ?", Q, mi.table, Q, Q, setColumns, Q, Q, pkName, Q)

	d.ins.ReplaceMarks(&query)

	res, err := q.Exec(query, setValues...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

// delete one record.
func (d *dbBaseClickHouse) Delete(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (cnt interface{}, err error) {
	var whereCols []string
	var args []interface{}
	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return 0, err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return 0, ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ? AND %s", Q, Q)
	wheres := strings.Join(whereCols, sep)

	// query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = ?", Q, mi.table, Q, Q, wheres, Q)
	query := fmt.Sprintf("ALTER TABLE %s%s%s DELETE WHERE %s%s%s = ?", Q, mi.table, Q, Q, wheres, Q)

	d.ins.ReplaceMarks(&query)
	res, err := q.Exec(query, args...)
	if err == nil {
		num, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if num > 0 {
			if mi.fields.pk.auto {
				if mi.fields.pk.fieldType&IsPositiveIntegerField > 0 {
					ind.FieldByIndex(mi.fields.pk.fieldIndex).SetUint(0)
				} else {
					ind.FieldByIndex(mi.fields.pk.fieldIndex).SetInt(0)
				}
			}
			err := d.deleteRels(q, mi, args, tz)
			if err != nil {
				return num, err
			}
		}
		return num, err
	}
	return 0, err
}

// do UpdateBanch or DeleteBanch by condition of tables' relationship.
func (d *dbBaseClickHouse) deleteRels(q dbQuerier, mi *modelInfo, args []interface{}, tz *time.Location) error {
	for _, fi := range mi.fields.fieldsReverse {
		fi = fi.reverseFieldInfo
		switch fi.onDelete {
		case odCascade:
			cond := NewCondition().And(fmt.Sprintf("%s__in", fi.name), args...)
			_, err := d.DeleteBatch(q, nil, fi.mi, cond, tz)
			if err != nil {
				return err
			}
		case odSetDefault, odSetNULL:
			cond := NewCondition().And(fmt.Sprintf("%s__in", fi.name), args...)
			params := Params{fi.column: nil}
			if fi.onDelete == odSetDefault {
				params[fi.column] = fi.initial.String()
			}
			_, err := d.UpdateBatch(q, nil, fi.mi, cond, OpDefault, params, tz)
			if err != nil {
				return err
			}
		case odDoNothing:
		}
	}
	return nil
}

// get indexview.
func (d *dbBaseClickHouse) Indexes(qs *querySet, mi *modelInfo, tz *time.Location) (iv IndexViewer) {
	return
}

// query sql, read values , save to *[]ParamList.
func (d *dbBaseClickHouse) ReadValues(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, exprs []string, container interface{}, tz *time.Location) (int64, error) {

	var (
		maps  []Params
		lists []ParamsList
		list  ParamsList
	)

	typ := 0
	switch v := container.(type) {
	case *[]Params:
		d := *v
		if len(d) == 0 {
			maps = d
		}
		typ = 1
	case *[]ParamsList:
		d := *v
		if len(d) == 0 {
			lists = d
		}
		typ = 2
	case *ParamsList:
		d := *v
		if len(d) == 0 {
			list = d
		}
		typ = 3
	default:
		panic(fmt.Errorf("unsupport read values type `%T`", container))
	}

	tables := newDbTables(mi, d.ins)

	var (
		cols  []string
		infos []*fieldInfo
	)

	hasExprs := len(exprs) > 0

	Q := d.ins.TableQuote()

	if hasExprs {
		cols = make([]string, 0, len(exprs))
		infos = make([]*fieldInfo, 0, len(exprs))
		for _, ex := range exprs {
			index, name, fi, suc := tables.parseExprs(mi, strings.Split(ex, ExprSep))
			if !suc {
				panic(fmt.Errorf("unknown field/column name `%s`", ex))
			}
			cols = append(cols, fmt.Sprintf("%s.%s%s%s %s%s%s", index, Q, fi.column, Q, Q, name, Q))
			infos = append(infos, fi)
		}
	} else {
		cols = make([]string, 0, len(mi.fields.dbcols))
		infos = make([]*fieldInfo, 0, len(exprs))
		for _, fi := range mi.fields.fieldsDB {
			cols = append(cols, fmt.Sprintf("T0.%s%s%s %s%s%s", Q, fi.column, Q, Q, fi.name, Q))
			infos = append(infos, fi)
		}
	}

	where, args := tables.getCondSQL(cond, false, tz)
	groupBy := tables.getGroupSQL(qs.groups)
	orderBy := tables.getOrderSQL(qs.orders)
	limit := tables.getLimitSQL(mi, qs.offset, qs.limit)
	join := tables.getJoinSQL()

	sels := strings.Join(cols, ", ")

	sqlSelect := "SELECT"
	if qs.distinct {
		sqlSelect += " DISTINCT"
	}
	query := fmt.Sprintf("%s %s FROM %s%s%s T0 %s%s%s%s%s", sqlSelect, sels, Q, mi.table, Q, join, where, groupBy, orderBy, limit)

	d.ins.ReplaceMarks(&query)

	rs, err := q.Query(query, args...)
	if err != nil {
		return 0, err
	}
	refs := make([]interface{}, len(cols))
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	defer rs.Close()

	var (
		cnt     int64
		columns []string
	)
	for rs.Next() {
		if cnt == 0 {
			cols, err := rs.Columns()
			if err != nil {
				return 0, err
			}
			columns = cols
		}

		if err := rs.Scan(refs...); err != nil {
			return 0, err
		}

		switch typ {
		case 1:
			params := make(Params, len(cols))
			for i, ref := range refs {
				fi := infos[i]

				val := reflect.Indirect(reflect.ValueOf(ref)).Interface()

				value, err := d.convertValueFromDB(fi, val, tz)
				if err != nil {
					panic(fmt.Errorf("db value convert failed `%v` %s", val, err.Error()))
				}

				params[columns[i]] = value
			}
			maps = append(maps, params)
		case 2:
			params := make(ParamsList, 0, len(cols))
			for i, ref := range refs {
				fi := infos[i]

				val := reflect.Indirect(reflect.ValueOf(ref)).Interface()

				value, err := d.convertValueFromDB(fi, val, tz)
				if err != nil {
					panic(fmt.Errorf("db value convert failed `%v` %s", val, err.Error()))
				}

				params = append(params, value)
			}
			lists = append(lists, params)
		case 3:
			for i, ref := range refs {
				fi := infos[i]

				val := reflect.Indirect(reflect.ValueOf(ref)).Interface()

				value, err := d.convertValueFromDB(fi, val, tz)
				if err != nil {
					panic(fmt.Errorf("db value convert failed `%v` %s", val, err.Error()))
				}

				list = append(list, value)
			}
		}

		cnt++
	}

	switch v := container.(type) {
	case *[]Params:
		*v = maps
	case *[]ParamsList:
		*v = lists
	case *ParamsList:
		*v = list
	}

	return cnt, nil
}
