package orm

import (
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

// read one record.
func (d *dbBaseClickHouse) FindOne(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)
	opt := options.FindOne()
	if len(cols) > 0 {
		projection := bson.M{}
		for _, col := range cols {
			projection[col] = 1
		}
		opt.SetProjection(projection)
	}

	if len(qs.orders) > 0 {
		opt.SetSort(getSort(qs.orders))
	}

	if qs.offset != 0 {
		opt.SetSkip(qs.offset)
	}

	filter := convertCondition(cond)

	if qs != nil && qs.forContext {
		err = col.FindOne(qs.ctx, filter, opt).Decode(container)
	} else {
		err = col.FindOne(todo, filter, opt).Decode(container)
	}

	return
}

// read one record.
func (d *dbBaseClickHouse) Distinct(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location, field string) (res []interface{}, err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)
	opt := options.Distinct()

	filter := convertCondition(cond)

	if qs != nil && qs.forContext {
		return col.Distinct(qs.ctx, field, filter, opt)
	} else {
		return col.Distinct(todo, field, filter, opt)
	}
}

// read all records.
func (d *dbBaseClickHouse) ReadBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.Find()
	if len(cols) > 0 {
		projection := bson.M{}
		for _, col := range cols {
			projection[col] = 1
		}
		opt.SetProjection(projection)
	}

	if len(qs.orders) > 0 {
		opt.SetSort(getSort(qs.orders))
	}

	if qs.limit != 0 {
		opt.SetLimit(qs.limit)
	}

	if qs.offset != 0 {
		opt.SetSkip(qs.offset)
	}

	filter := convertCondition(cond)
	cur := &mongo.Cursor{}
	if qs != nil && qs.forContext {
		// Do something with content
		cur, err = col.Find(qs.ctx, filter, opt)
	} else {
		// Do something without content
		cur, err = col.Find(todo, filter, opt)
	}
	// defer cur.Close(todo)
	if err != nil {
		return
	}
	err = cur.All(todo, container)

	return
}

// get the recodes count.
func (d *dbBaseClickHouse) Count(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (i int64, err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.Count()

	filter := convertCondition(cond)

	if qs != nil && qs.forContext {
		if len(filter) == 0 {
			i, err = col.EstimatedDocumentCount(qs.ctx, nil)
		} else {
			i, err = col.CountDocuments(qs.ctx, filter, opt)
		}
	} else {
		// Do something without content
		if len(filter) == 0 {
			i, err = col.EstimatedDocumentCount(todo, nil)
		} else {
			i, err = col.CountDocuments(todo, filter, opt)
		}
	}

	return
}

// update the recodes.
func (d *dbBaseClickHouse) UpdateBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, operator OperatorUpdate, params Params, tz *time.Location) (i int64, err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.Update()

	filter := convertCondition(cond)
	update := bson.M{}
	for col, val := range params {
		// if fi, ok := mi.fields.GetByAny(col); !ok || !fi.dbcol {
		// 	panic(fmt.Errorf("wrong field/column name `%s`", col))
		// } else {
		update[col] = val
		// }
	}
	update = bson.M{
		string(operator): update,
	}
	r := &mongo.UpdateResult{}
	if qs != nil && qs.forContext {
		r, err = col.UpdateMany(qs.ctx, filter, update, opt)
	} else {
		// Do something without content
		r, err = col.UpdateMany(todo, filter, update, opt)
	}
	if err != nil {
		return
	}

	i = r.ModifiedCount

	return
}

// delete the recodes.
func (d *dbBaseClickHouse) DeleteBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, tz *time.Location) (i int64, err error) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.Delete()

	filter := convertCondition(cond)

	r := &mongo.DeleteResult{}
	if qs != nil && qs.forContext {
		r, err = col.DeleteMany(qs.ctx, filter, opt)
	} else {
		// Do something without content
		r, err = col.DeleteMany(todo, filter, opt)
	}
	if err != nil {
		return
	}

	i = r.DeletedCount

	return
}

// read one record.
func (d *dbBaseClickHouse) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location, cols []string, isForUpdate bool) (err error) {
	db := q.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.FindOne()

	var whereCols []string
	var args []interface{}
	if len(cols) > 0 {
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

	filter := bson.M{}
	for i, p := range whereCols {
		filter[p] = args[i]
	}
	// Do something without content
	data, err := col.FindOne(todo, filter, opt).DecodeBytes()
	if err != nil {
		return err
	}
	err = bson.Unmarshal(data, container)

	return
}

// insert one record.
func (d *dbBaseClickHouse) InsertOne(q dbQuerier, mi *modelInfo, ind reflect.Value, container interface{}, tz *time.Location) (id interface{}, err error) {
	db := q.(*DB).MDB
	col := db.Collection(mi.table)
	_, _, b := getExistPk(mi, ind)
	name := mi.fields.pk.name

	if !b {
		reflect.ValueOf(container).Elem().FieldByName(name).SetString(primitive.NewObjectID().Hex())
	}

	opt := options.InsertOne()

	// Do something without content
	data, err := col.InsertOne(todo, container, opt)
	if err != nil {
		return
	}
	id = data.InsertedID
	return
}

// insert all records.
func (d *dbBaseClickHouse) InsertMulti(q dbQuerier, mi *modelInfo, ind reflect.Value, bulk int, containers interface{}, tz *time.Location) (ids interface{}, err error) {
	db := q.(*DB).MDB
	col := db.Collection(mi.table)
	_, _, b := getExistPk(mi, ind)
	name := mi.fields.pk.name
	sind := reflect.Indirect(reflect.ValueOf(containers))

	cs := []interface{}{}

	if !b {
		for i := 0; i < sind.Len(); i++ {
			c := reflect.Indirect(sind.Index(i))
			c.FieldByName(name).SetString(primitive.NewObjectID().Hex())
			cs = append(cs, c.Interface())
		}
	}

	opt := options.InsertMany()

	// Do something without content
	data, err := col.InsertMany(todo, cs, opt)
	if err != nil {
		return
	}
	ids = data.InsertedIDs
	return
}

// update one record.
func (d *dbBaseClickHouse) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (id interface{}, err error) {
	db := q.(*DB).MDB
	col := db.Collection(mi.table)
	c, val, b := getExistPk(mi, ind)
	if !b {
		return nil, ErrHaveNoPK
	}

	opt := options.Update()
	var whereCols []string
	var args []interface{}

	if len(cols) == 0 {
		cols = mi.fields.dbcols
	}
	whereCols = make([]string, 0, len(cols))
	args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
	if err != nil {
		return
	}

	filter := bson.M{
		c: val,
	}

	update := bson.M{}
	for i, p := range whereCols {
		if p != c {
			update[p] = args[i]
		}
	}

	update = bson.M{
		"$set": update,
	}

	// Do something without content
	data, err := col.UpdateOne(todo, filter, update, opt)
	id = data.UpsertedID
	return
}

// delete one record.
func (d *dbBaseClickHouse) Delete(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (cnt interface{}, err error) {
	db := q.(*DB).MDB
	col := db.Collection(mi.table)

	opt := options.Delete()
	var whereCols []string
	var args []interface{}
	if len(cols) > 0 {
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return nil, ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	filter := bson.M{}
	for i, p := range whereCols {
		filter[p] = args[i]
	}

	// Do something without content
	data, err := col.DeleteOne(todo, filter, opt)
	cnt = data.DeletedCount
	return
}

// get indexview.
func (d *dbBaseClickHouse) Indexes(qs *querySet, mi *modelInfo, tz *time.Location) (iv IndexViewer) {
	db := qs.orm.db.(*DB).MDB
	col := db.Collection(mi.table)

	return newIndexView(col.Indexes())
}
