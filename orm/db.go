package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type OperatorUpdate string

var (
	DefaulrOperatorUpdate OperatorUpdate = "$set"
)

var (
	operators = map[string]bool{
		"exact":       true,
		"iexact":      true,
		"contains":    true,
		"icontains":   true,
		"gt":          true,
		"gte":         true,
		"lt":          true,
		"lte":         true,
		"eq":          true,
		"nq":          true,
		"ne":          true,
		"startswith":  true,
		"endswith":    true,
		"istartswith": true,
		"iendswith":   true,
		"in":          true,
		"between":     true,
		"isnull":      true,
		"regex":       true,
	}
)

type dbBase struct {
	ins dbBaser
}

var _ dbBaser = new(dbBase)

// convert time from db.
func (d *dbBase) TimeFromDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

// convert time to db.
func (d *dbBase) TimeToDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

// generate sql with replacing operator string placeholders and replaced values.
func (d *dbBase) GenerateOperatorSQL(mi *modelInfo, fi *fieldInfo, operator string, args []interface{}, tz *time.Location) (string, []interface{}) {
	var sql string
	params := getFlatParams(fi, args, tz)

	if len(params) == 0 {
		panic(fmt.Errorf("operator `%s` need at least one args", operator))
	}
	arg := params[0]

	switch operator {
	case "in":
		marks := make([]string, len(params))
		for i := range marks {
			marks[i] = "?"
		}
		sql = fmt.Sprintf("IN (%s)", strings.Join(marks, ", "))
	case "between":
		if len(params) != 2 {
			panic(fmt.Errorf("operator `%s` need 2 args not %d", operator, len(params)))
		}
		sql = "BETWEEN ? AND ?"
	default:
		if len(params) > 1 {
			panic(fmt.Errorf("operator `%s` need 1 args not %d", operator, len(params)))
		}
		sql = d.ins.OperatorSQL(operator)
		switch operator {
		case "exact":
			if arg == nil {
				params[0] = "IS NULL"
			}
		case "iexact", "contains", "icontains", "startswith", "endswith", "istartswith", "iendswith":
			param := strings.Replace(ToStr(arg), `%`, `\%`, -1)
			switch operator {
			case "iexact":
			case "contains", "icontains":
				param = fmt.Sprintf("%%%s%%", param)
			case "startswith", "istartswith":
				param = fmt.Sprintf("%s%%", param)
			case "endswith", "iendswith":
				param = fmt.Sprintf("%%%s", param)
			}
			params[0] = param
		case "isnull":
			if b, ok := arg.(bool); ok {
				if b {
					sql = "IS NULL"
				} else {
					sql = "IS NOT NULL"
				}
				params = nil
			} else {
				panic(fmt.Errorf("operator `%s` need a bool value not `%T`", operator, arg))
			}
		}
	}
	return sql, params
}

// gernerate sql string with inner function, such as UPPER(text).
func (d *dbBase) GenerateOperatorLeftCol(*fieldInfo, string, *string) {
	// default not use
}

// get struct columns values as interface slice.
func (d *dbBase) collectValues(mi *modelInfo, ind reflect.Value, cols []string, skipAuto bool, insert bool, names *[]string, tz *time.Location) (values []interface{}, autoFields []string, err error) {
	if names == nil {
		ns := make([]string, 0, len(cols))
		names = &ns
	}
	values = make([]interface{}, 0, len(cols))

	for _, column := range cols {
		var fi *fieldInfo
		if fi, _ = mi.fields.GetByAny(column); fi != nil {
			column = fi.column
		} else {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", column, mi.fullName))
		}
		if !fi.dbcol || fi.auto && skipAuto {
			continue
		}
		value, err := d.collectFieldValue(mi, fi, ind, insert, tz)
		if err != nil {
			return nil, nil, err
		}

		// ignore empty value auto field
		if insert && fi.auto {
			if fi.fieldType&IsPositiveIntegerField > 0 {
				if vu, ok := value.(uint64); !ok || vu == 0 {
					continue
				}
			} else {
				if vu, ok := value.(int64); !ok || vu == 0 {
					continue
				}
			}
			autoFields = append(autoFields, fi.column)
		}

		*names, values = append(*names, column), append(values, value)
	}

	return
}

// get one field value in struct column as interface.
func (d *dbBase) collectFieldValue(mi *modelInfo, fi *fieldInfo, ind reflect.Value, insert bool, tz *time.Location) (interface{}, error) {
	var value interface{}
	if fi.pk {
		_, value, _ = getExistPk(mi, ind)
	} else {
		field := ind.FieldByIndex(fi.fieldIndex)
		if fi.isFielder {
			f := field.Addr().Interface().(Fielder)
			value = f.RawValue()
		} else {
			switch fi.fieldType {
			case TypeBooleanField:
				if nb, ok := field.Interface().(sql.NullBool); ok {
					value = nil
					if nb.Valid {
						value = nb.Bool
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().Bool()
					}
				} else {
					value = field.Bool()
				}
			case TypeVarCharField, TypeCharField, TypeTextField, TypeJSONField, TypeJsonbField:
				if ns, ok := field.Interface().(sql.NullString); ok {
					value = nil
					if ns.Valid {
						value = ns.String
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().String()
					}
				} else {
					value = field.String()
				}
			case TypeFloatField, TypeDecimalField:
				if nf, ok := field.Interface().(sql.NullFloat64); ok {
					value = nil
					if nf.Valid {
						value = nf.Float64
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().Float()
					}
				} else {
					vu := field.Interface()
					if _, ok := vu.(float32); ok {
						value, _ = StrTo(ToStr(vu)).Float64()
					} else {
						value = field.Float()
					}
				}
			case TypeTimeField, TypeDateField, TypeDateTimeField:
				value = field.Interface()
				if t, ok := value.(time.Time); ok {
					d.ins.TimeToDB(&t, tz)
					if t.IsZero() {
						value = nil
					} else {
						value = t
					}
				}
			default:
				switch {
				case fi.fieldType&IsPositiveIntegerField > 0:
					if field.Kind() == reflect.Ptr {
						if field.IsNil() {
							value = nil
						} else {
							value = field.Elem().Uint()
						}
					} else {
						value = field.Uint()
					}
				case fi.fieldType&IsIntegerField > 0:
					if ni, ok := field.Interface().(sql.NullInt64); ok {
						value = nil
						if ni.Valid {
							value = ni.Int64
						}
					} else if field.Kind() == reflect.Ptr {
						if field.IsNil() {
							value = nil
						} else {
							value = field.Elem().Int()
						}
					} else {
						value = field.Int()
					}
				case fi.fieldType&IsRelField > 0:
					if field.IsNil() {
						value = nil
					} else {
						if _, vu, ok := getExistPk(fi.relModelInfo, reflect.Indirect(field)); ok {
							value = vu
						} else {
							value = nil
						}
					}
					if !fi.null && value == nil {
						return nil, fmt.Errorf("field `%s` cannot be NULL", fi.fullName)
					}
				}
			}
		}
		switch fi.fieldType {
		case TypeTimeField, TypeDateField, TypeDateTimeField:
			if fi.autoNow || fi.autoNowAdd && insert {
				if insert {
					if t, ok := value.(time.Time); ok && !t.IsZero() {
						break
					}
				}
				tnow := time.Now()
				d.ins.TimeToDB(&tnow, tz)
				value = tnow
				if fi.isFielder {
					f := field.Addr().Interface().(Fielder)
					f.SetRaw(tnow.In(DefaultTimeLoc))
				} else if field.Kind() == reflect.Ptr {
					v := tnow.In(DefaultTimeLoc)
					field.Set(reflect.ValueOf(&v))
				} else {
					field.Set(reflect.ValueOf(tnow.In(DefaultTimeLoc)))
				}
			}
		case TypeJSONField, TypeJsonbField:
			if s, ok := value.(string); (ok && len(s) == 0) || value == nil {
				if fi.colDefault && fi.initial.Exist() {
					value = fi.initial.String()
				} else {
					value = nil
				}
			}
		}
	}
	return value, nil
}

// set values to struct column.
func (d *dbBase) setColsValues(mi *modelInfo, ind *reflect.Value, cols []string, values []interface{}, tz *time.Location) {
	for i, column := range cols {
		val := reflect.Indirect(reflect.ValueOf(values[i])).Interface()

		fi := mi.fields.GetByColumn(column)

		field := ind.FieldByIndex(fi.fieldIndex)

		value, err := d.convertValueFromDB(fi, val, tz)
		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}

		_, err = d.setFieldValue(fi, value, field)

		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}
	}
}

// convert value from database result to value following in field type.
func (d *dbBase) convertValueFromDB(fi *fieldInfo, val interface{}, tz *time.Location) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	var value interface{}
	var tErr error

	var str *StrTo
	switch v := val.(type) {
	case []byte:
		s := StrTo(string(v))
		str = &s
	case string:
		s := StrTo(v)
		str = &s
	}

	fieldType := fi.fieldType

setValue:
	switch {
	case fieldType == TypeBooleanField:
		if str == nil {
			switch v := val.(type) {
			case int64:
				b := v == 1
				value = b
			default:
				s := StrTo(ToStr(v))
				str = &s
			}
		}
		if str != nil {
			b, err := str.Bool()
			if err != nil {
				tErr = err
				goto end
			}
			value = b
		}
	case fieldType == TypeVarCharField || fieldType == TypeCharField || fieldType == TypeTextField || fieldType == TypeJSONField || fieldType == TypeJsonbField:
		if str == nil {
			value = ToStr(val)
		} else {
			value = str.String()
		}
	case fieldType == TypeTimeField || fieldType == TypeDateField || fieldType == TypeDateTimeField:
		if str == nil {
			switch t := val.(type) {
			case time.Time:
				d.ins.TimeFromDB(&t, tz)
				value = t
			default:
				s := StrTo(ToStr(t))
				str = &s
			}
		}
		if str != nil {
			s := str.String()
			var (
				t   time.Time
				err error
			)
			if len(s) >= 19 {
				s = s[:19]
				t, err = time.ParseInLocation(formatDateTime, s, tz)
			} else if len(s) >= 10 {
				if len(s) > 10 {
					s = s[:10]
				}
				t, err = time.ParseInLocation(formatDate, s, tz)
			} else if len(s) >= 8 {
				if len(s) > 8 {
					s = s[:8]
				}
				t, err = time.ParseInLocation(formatTime, s, tz)
			}
			t = t.In(DefaultTimeLoc)

			if err != nil && s != "00:00:00" && s != "0000-00-00" && s != "0000-00-00 00:00:00" {
				tErr = err
				goto end
			}
			value = t
		}
	case fieldType&IsIntegerField > 0:
		if str == nil {
			s := StrTo(ToStr(val))
			str = &s
		}
		if str != nil {
			var err error
			switch fieldType {
			case TypeBitField:
				_, err = str.Int8()
			case TypeSmallIntegerField:
				_, err = str.Int16()
			case TypeIntegerField:
				_, err = str.Int32()
			case TypeBigIntegerField:
				_, err = str.Int64()
			case TypePositiveBitField:
				_, err = str.Uint8()
			case TypePositiveSmallIntegerField:
				_, err = str.Uint16()
			case TypePositiveIntegerField:
				_, err = str.Uint32()
			case TypePositiveBigIntegerField:
				_, err = str.Uint64()
			}
			if err != nil {
				tErr = err
				goto end
			}
			if fieldType&IsPositiveIntegerField > 0 {
				v, _ := str.Uint64()
				value = v
			} else {
				v, _ := str.Int64()
				value = v
			}
		}
	case fieldType == TypeFloatField || fieldType == TypeDecimalField:
		if str == nil {
			switch v := val.(type) {
			case float64:
				value = v
			default:
				s := StrTo(ToStr(v))
				str = &s
			}
		}
		if str != nil {
			v, err := str.Float64()
			if err != nil {
				tErr = err
				goto end
			}
			value = v
		}
	case fieldType&IsRelField > 0:
		fi = fi.relModelInfo.fields.pk
		fieldType = fi.fieldType
		goto setValue
	}

end:
	if tErr != nil {
		err := fmt.Errorf("convert to `%s` failed, field: %s err: %s", fi.addrValue.Type(), fi.fullName, tErr)
		return nil, err
	}

	return value, nil

}

// set one value to struct column field.
func (d *dbBase) setFieldValue(fi *fieldInfo, value interface{}, field reflect.Value) (interface{}, error) {

	fieldType := fi.fieldType
	isNative := !fi.isFielder

setValue:
	switch {
	case fieldType == TypeBooleanField:
		if isNative {
			if nb, ok := field.Interface().(sql.NullBool); ok {
				if value == nil {
					nb.Valid = false
				} else {
					nb.Bool = value.(bool)
					nb.Valid = true
				}
				field.Set(reflect.ValueOf(nb))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(bool)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				if value == nil {
					value = false
				}
				field.SetBool(value.(bool))
			}
		}
	case fieldType == TypeVarCharField || fieldType == TypeCharField || fieldType == TypeTextField || fieldType == TypeJSONField || fieldType == TypeJsonbField:
		if isNative {
			if ns, ok := field.Interface().(sql.NullString); ok {
				if value == nil {
					ns.Valid = false
				} else {
					ns.String = value.(string)
					ns.Valid = true
				}
				field.Set(reflect.ValueOf(ns))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(string)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				if value == nil {
					value = ""
				}
				field.SetString(value.(string))
			}
		}
	case fieldType == TypeTimeField || fieldType == TypeDateField || fieldType == TypeDateTimeField:
		if isNative {
			if value == nil {
				value = time.Time{}
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(time.Time)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				field.Set(reflect.ValueOf(value))
			}
		}
	case fieldType == TypePositiveBitField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := uint8(value.(uint64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypePositiveSmallIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := uint16(value.(uint64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypePositiveIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			if field.Type() == reflect.TypeOf(new(uint)) {
				v := uint(value.(uint64))
				field.Set(reflect.ValueOf(&v))
			} else {
				v := uint32(value.(uint64))
				field.Set(reflect.ValueOf(&v))
			}
		}
	case fieldType == TypePositiveBigIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := value.(uint64)
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeBitField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := int8(value.(int64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeSmallIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := int16(value.(int64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			if field.Type() == reflect.TypeOf(new(int)) {
				v := int(value.(int64))
				field.Set(reflect.ValueOf(&v))
			} else {
				v := int32(value.(int64))
				field.Set(reflect.ValueOf(&v))
			}
		}
	case fieldType == TypeBigIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := value.(int64)
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType&IsIntegerField > 0:
		if fieldType&IsPositiveIntegerField > 0 {
			if isNative {
				if value == nil {
					value = uint64(0)
				}
				field.SetUint(value.(uint64))
			}
		} else {
			if isNative {
				if ni, ok := field.Interface().(sql.NullInt64); ok {
					if value == nil {
						ni.Valid = false
					} else {
						ni.Int64 = value.(int64)
						ni.Valid = true
					}
					field.Set(reflect.ValueOf(ni))
				} else {
					if value == nil {
						value = int64(0)
					}
					field.SetInt(value.(int64))
				}
			}
		}
	case fieldType == TypeFloatField || fieldType == TypeDecimalField:
		if isNative {
			if nf, ok := field.Interface().(sql.NullFloat64); ok {
				if value == nil {
					nf.Valid = false
				} else {
					nf.Float64 = value.(float64)
					nf.Valid = true
				}
				field.Set(reflect.ValueOf(nf))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					if field.Type() == reflect.TypeOf(new(float32)) {
						v := float32(value.(float64))
						field.Set(reflect.ValueOf(&v))
					} else {
						v := value.(float64)
						field.Set(reflect.ValueOf(&v))
					}
				}
			} else {

				if value == nil {
					value = float64(0)
				}
				field.SetFloat(value.(float64))
			}
		}
	case fieldType&IsRelField > 0:
		if value != nil {
			fieldType = fi.relModelInfo.fields.pk.fieldType
			mf := reflect.New(fi.relModelInfo.addrField.Elem().Type())
			field.Set(mf)
			f := mf.Elem().FieldByIndex(fi.relModelInfo.fields.pk.fieldIndex)
			field = f
			goto setValue
		}
	}

	if !isNative {
		fd := field.Addr().Interface().(Fielder)
		err := fd.SetRaw(value)
		if err != nil {
			err = fmt.Errorf("converted value `%v` set to Fielder `%s` failed, err: %s", value, fi.fullName, err)
			return nil, err
		}
	}

	return value, nil
}

// read one record.
func (d *dbBase) FindOne(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
	mi, ind := qs.orm.getMiInd(container, true)
	err = d.Read(q, mi, ind, container, qs.orm.alias.TZ, cols, false)
	return
}

// read one record.
func (d *dbBase) Distinct(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location, field string) (res []interface{}, err error) {
	return
}

// read all records.
func (d *dbBase) ReadBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
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
	r, err := q.Query(query, args...)
	if err != nil {
		return err
	}
	rs = r

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
func (d *dbBase) Count(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (cnt int64, err error) {
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

	row := q.QueryRow(query, args...)

	err = row.Scan(&cnt)
	return
}

// get indexview.
func (d *dbBase) Indexes(qs *querySet, mi *modelInfo, tz *time.Location) (iv IndexViewer) {
	return
}

// read one record.
func (d *dbBase) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location, cols []string, isForUpdate bool) (err error) {
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
func (d *dbBase) InsertOne(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location) (id interface{}, err error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}

	id, err = d.InsertValue(q, mi, false, names, values)
	if err != nil {
		return 0, err
	}

	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}
	return id, err
}

// insert all records.
func (d *dbBase) InsertMulti(q dbQuerier, mi *modelInfo, ind reflect.Value, bulk int, containers interface{}, tz *time.Location) (ids interface{}, err error) {
	var (
		cnt    int64
		nums   int
		values []interface{}
		names  []string
	)

	// typ := reflect.Indirect(mi.addrField).Type()

	length, autoFields := ind.Len(), make([]string, 0, 1)

	for i := 1; i <= length; i++ {

		ind := reflect.Indirect(ind.Index(i - 1))

		// Is this needed ?
		// if !ind.Type().AssignableTo(typ) {
		// 	return cnt, ErrArgs
		// }

		if i == 1 {
			var (
				vus []interface{}
				err error
			)
			vus, autoFields, err = d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
			if err != nil {
				return cnt, err
			}
			values = make([]interface{}, bulk*len(vus))
			nums += copy(values, vus)
		} else {
			vus, _, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, nil, tz)
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
				return cnt, err
			}
			cnt += num
			nums = 0
		}
	}

	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}

	return cnt, err
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
func (d *dbBase) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (int64, error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	for i := range marks {
		marks[i] = "?"
	}

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)

	if isMulti {
		qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	}

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)

	d.ins.ReplaceMarks(&query)

	if isMulti || !d.ins.HasReturningID(mi, &query) {
		res, err := q.Exec(query, values...)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return res.LastInsertId()
		}
		return 0, err
	}
	row := q.QueryRow(query, values...)
	var id int64
	err := row.Scan(&id)
	return id, err
}

// update one record.
func (d *dbBase) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (id interface{}, err error) {
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
func (d *dbBase) Delete(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (cnt interface{}, err error) {
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

	query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = ?", Q, mi.table, Q, Q, wheres, Q)

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

// delete related records.
// do UpdateBanch or DeleteBanch by condition of tables' relationship.
func (d *dbBase) deleteRels(q dbQuerier, mi *modelInfo, args []interface{}, tz *time.Location) error {
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
			_, err := d.UpdateBatch(q, nil, fi.mi, cond, DefaulrOperatorUpdate, params, tz)
			if err != nil {
				return err
			}
		case odDoNothing:
		}
	}
	return nil
}

// delete table-related records.
func (d *dbBase) DeleteBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (int64, error) {
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
	join := tables.getJoinSQL()

	cols := fmt.Sprintf("T0.%s%s%s", Q, mi.fields.pk.column, Q)
	query := fmt.Sprintf("SELECT %s FROM %s%s%s T0 %s%s", cols, Q, mi.table, Q, join, where)

	d.ins.ReplaceMarks(&query)

	var rs *sql.Rows
	r, err := q.Query(query, args...)
	if err != nil {
		return 0, err
	}
	rs = r
	defer rs.Close()

	var ref interface{}
	args = make([]interface{}, 0)
	cnt := 0
	for rs.Next() {
		if err := rs.Scan(&ref); err != nil {
			return 0, err
		}
		pkValue, err := d.convertValueFromDB(mi.fields.pk, reflect.ValueOf(ref).Interface(), tz)
		if err != nil {
			return 0, err
		}
		args = append(args, pkValue)
		cnt++
	}

	if cnt == 0 {
		return 0, nil
	}

	marks := make([]string, len(args))
	for i := range marks {
		marks[i] = "?"
	}
	sql := fmt.Sprintf("IN (%s)", strings.Join(marks, ", "))
	query = fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s %s", Q, mi.table, Q, Q, mi.fields.pk.column, Q, sql)

	d.ins.ReplaceMarks(&query)
	res, err := q.Exec(query, args...)
	if err == nil {
		num, err := res.RowsAffected()
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
	return 0, err
}

// update table-related record by querySet.
// need querySet not struct reflect.Value to update related records.
func (d *dbBase) UpdateBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, op OperatorUpdate, params Params, tz *time.Location) (int64, error) {
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

	var query, T string

	Q := d.ins.TableQuote()

	if d.ins.SupportUpdateJoin() {
		T = "T0."
	}

	cols := make([]string, 0, len(columns))

	for i, v := range columns {
		col := fmt.Sprintf("%s%s%s%s", T, Q, v, Q)
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

	if d.ins.SupportUpdateJoin() {
		query = fmt.Sprintf("UPDATE %s%s%s T0 %sSET %s%s", Q, mi.table, Q, join, sets, where)
	} else {
		supQuery := fmt.Sprintf("SELECT T0.%s%s%s FROM %s%s%s T0 %s%s", Q, mi.fields.pk.column, Q, Q, mi.table, Q, join, where)
		query = fmt.Sprintf("UPDATE %s%s%s SET %sWHERE %s%s%s IN ( %s )", Q, mi.table, Q, sets, Q, mi.fields.pk.column, Q, supQuery)
	}

	d.ins.ReplaceMarks(&query)
	res, err := q.Exec(query, values...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

// return quote.
func (d *dbBase) TableQuote() string {
	return "`"
}

// replace value placeholder in parametered sql string.
func (d *dbBase) ReplaceMarks(query *string) {
	// default use `?` as mark, do nothing
}

// flag of RETURNING sql.
func (d *dbBase) HasReturningID(*modelInfo, *string) bool {
	return false
}

// sync auto key
func (d *dbBase) setval(db dbQuerier, mi *modelInfo, autoFields []string) error {
	return nil
}

// flag of update joined record.
func (d *dbBase) SupportUpdateJoin() bool {
	return true
}

func (d *dbBase) MaxLimit() uint64 {
	return 18446744073709551615
}

// not implement.
func (d *dbBase) OperatorSQL(operator string) string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) ShowTablesQuery() string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) ShowColumnsQuery(table string) string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) IndexExists(dbQuerier, string, string) bool {
	panic(ErrNotImplement)
}
