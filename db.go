package spcdb

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"github.com/mitchellh/mapstructure"
)

var (
    AttributeName = "mapstructure"
    TimeFormat = "2006-01-02 15:04:05"
)

type DBMediator interface {
	DB() *DB
}

type DB struct {
	*sql.DB
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

type Record interface {
	Get(key string) interface{}
	GetRaw(key string) *reflect.Value
	GetInString(key string) string
	Each(func(key string, value reflect.Value))
	Merge(r Record)
	Model(dst interface{}) error
}

type record struct {
	raw map[string]reflect.Value
	m   sync.RWMutex
}

func recFromMap(recMap reflect.Value, recType reflect.Type, dst map[string]reflect.Value) {
    dstVal := reflect.Indirect(reflect.ValueOf(dst))
	for _, k := range recMap.MapKeys() {
        dstVal.SetMapIndex(k, recMap.MapIndex(k))
    }
}

func recFromStruct(valStruct reflect.Value, typeStruct reflect.Type, dst map[string]reflect.Value) {
	for i := 0; i < typeStruct.NumField(); i++ {
		typeField := typeStruct.Field(i)
		if !unicode.IsUpper(rune(typeField.Name[0])) {
			continue
		}

		recName := typeField.Tag.Get(AttributeName)
		if recName == "" {
			if typeField.Anonymous {
				continue
			}
			recName = typeField.Name
		} else if recName == "-" {
			continue
		}

		structField := valStruct.Field(i)
		if !structField.IsValid() || !structField.CanInterface() {
			continue
		}
		dst[recName] = structField
	}
}

func recFrom(recObj reflect.Value, dst map[string]reflect.Value) {
	if recObj.Kind() == reflect.Ptr {
		recObj = recObj.Elem()
	}
	typeObj := recObj.Type()

	switch typeObj.Kind() {
		case reflect.Struct:
			recFromStruct(recObj, typeObj, dst)
		case reflect.Map:
			recFromMap(recObj, typeObj, dst)
	}
}

func NewRecord(mapToObj ...interface{}) Record {
	rec := &record{
		raw: make(map[string]reflect.Value),
	}
	for _, val := range mapToObj {
		if val == nil {
			continue
		}
		obj := normalizeValue(reflect.ValueOf(val))
		recFrom(obj, rec.raw)
	}
	return rec
}

func (r *record) String() string {
	if r == nil {
		return ""
	}
    s := make([]string, 0, len(r.raw))
	for key, _ := range r.raw {
        s = append(s, fmt.Sprintf("\"%s\":\"%s\"", key, r.GetInString(key)))
	}
	return "{" + strings.Join(s, ", ") + "}"
}

func (r *record) Get(key string) interface{} {
	if r == nil {
		return nil
	}
	r.m.RLock()
	defer r.m.RUnlock()
	if el, ok := r.raw[key]; ok && el.IsValid() {
		return el.Interface()
	}
	return nil
}

func (r *record) GetRaw(key string) *reflect.Value {
	if r == nil {
		return nil
	}
	r.m.RLock()
	defer r.m.RUnlock()
	el, ok := r.raw[key]
	if !ok {
		return nil
	}
	return &el
}

func (r *record) GetInString(key string) string {
	if r == nil {
		return ""
	}
	r.m.RLock()
	defer r.m.RUnlock()
	el, ok := r.raw[key]
	if !ok || !el.IsValid() {
		return ""
	}

	var str string
	//mapstructure.WeakDecode(el.Interface(), &str)
	//return str
	switch s := el.Interface().(type) {
	case string:
		str = s
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		str = fmt.Sprintf("%d", s)
	case float32, float64:
		str = fmt.Sprintf("%f", s)
	case []byte:
		str = string(s[:])
	case bool:
		str = strconv.FormatBool(s)
	case time.Time:
		str = s.Format(TimeFormat)
	default:
		str = fmt.Sprintf("%v", el.Interface())
	}

	return str
}

func (r *record) Each(fn func(key string, value reflect.Value)) {
	if r == nil {
		return
	}
	r.m.RLock()
	defer r.m.RUnlock()
	for key, value := range r.raw {
		fn(key, value)
	}
}

func (r *record) Merge(r2 Record) {
	if r == nil || r2 == nil {
		return
	}
	r.m.Lock()
	defer r.m.Unlock()
	r2.Each(func(key string, value reflect.Value) {
		r.raw[key] = value
	})
}

func (r *record) Model(rawVal interface{}) error {
	if r == nil {
		return nil
	}
	r.m.RLock()
	defer r.m.RUnlock()

    dataMap := make(map[string]interface{}, len(r.raw))
    for key, val := range r.raw {
        if !val.IsValid() {
            dataMap[key] = nil
        } else {
            dataMap[key] = val.Interface()
        }
    }
	return newModel(dataMap, rawVal)
}

func (db *DB) ExistsRecord(query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}
	return nil
}

func (db *DB) QueryModel(query string, model interface{}, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
    container, err := newContainer(rows, cols)
    if err != nil {
        return err
    }
    for key, val := range container {
        //container[key] = *(*interface{})(&val)
        container[key] = reflect.ValueOf(val).Elem().Interface()
    }
	return newModel(container, model)
}

func (db *DB) QueryRecords(query string, args ...interface{}) ([]Record, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	ret := make([]Record, 0, 10)
	for rows.Next() {
        rec, err := newRecord(rows, cols)
        if err != nil {
            return nil, err
        }
		ret = append(ret, rec)
	}
	return ret, err
}

func (db *DB) QueryRecord(query string, args ...interface{}) (Record, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	return newRecord(rows, cols)
}

func normalizeValue(value reflect.Value) reflect.Value {
	k := value.Kind()
	if k == reflect.Interface || k == reflect.Ptr {
		return normalizeValue(value.Elem())
	}
	return value
}

func newContainer(rows *sql.Rows, cols []string) (map[string]interface{}, error) {
	/*
		ptrs := make([]interface{}, len(cols))
		cont := make([]string, len(cols))
		for i, _ := range ptrs {
			ptrs[i] = &cont[i]
		}
		rows.Scan(ptrs...)
        return cont
	*/
	pointers := make([]interface{}, len(cols))
	container := make(map[string]interface{}, len(cols))
	for i, _ := range pointers {
		var v interface{}
		container[cols[i]] = &v
		pointers[i] = &v
	}
	err := rows.Scan(pointers...)
    return container, err
}

func newRecord(rows *sql.Rows, cols []string) (Record, error) {
    container, err := newContainer(rows, cols)
    if err != nil {
        return nil, err
    }
	rec := record{raw: make(map[string]reflect.Value, len(cols))}
	for key, value := range container {
		rec.raw[key] = reflect.Indirect(reflect.ValueOf(value)).Elem()
	}
	return &rec, nil
}

func newModel(src, dst interface{}) error {
	config := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           dst,
		WeaklyTypedInput: true,
        TagName:          AttributeName,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(src)
}
