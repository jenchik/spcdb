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
	//"github.com/mitchellh/mapstructure"
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
}

type record struct {
	raw map[string]reflect.Value
	m   sync.RWMutex
}

func mapToMap(recMap reflect.Value, recType reflect.Type, dst map[string]reflect.Value) {
	// TODO
}

func mapToStruct(valStruct reflect.Value, typeStruct reflect.Type, dst map[string]reflect.Value) {
	for i := 0; i < typeStruct.NumField(); i++ {
		typeField := typeStruct.Field(i)
		if !unicode.IsUpper(rune(typeField.Name[0])) {
			continue
		}

		recName := typeField.Tag.Get("record")
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

func mapTo(recObj reflect.Value, dst map[string]reflect.Value) {
	if recObj.Kind() == reflect.Ptr {
		recObj = recObj.Elem()
	}
	typeObj := recObj.Type()

	switch typeObj.Kind() {
		case reflect.Struct:
			mapToStruct(recObj, typeObj, dst)
		case reflect.Map:
			mapToMap(recObj, typeObj, dst)
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
		mapTo(obj, rec.raw)
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
	return "Record[" + strings.Join(s, ", ") + "]"
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
	case []byte:
		str = string(s[:])
	case bool:
		str = strconv.FormatBool(s)
	case time.Time:
		str = s.Format("2006-01-02 15:04:05")
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

func normalizeValue(value reflect.Value) reflect.Value {
	k := value.Kind()
	if k == reflect.Interface || k == reflect.Ptr {
		return normalizeValue(value.Elem())
	}
	return value
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

func newRecord(rows *sql.Rows, cols []string) Record {
	/*
		ptrs := make([]interface{}, len(cols))
		cont := make([]string, len(cols))
		for i, _ := range ptrs {
			ptrs[i] = &cont[i]
		}
		rows.Scan(ptrs...)
	*/
	pointers := make([]interface{}, len(cols))
	container := make(map[string]interface{}, len(cols))
	for i, _ := range pointers {
		var v interface{}
		container[cols[i]] = &v
		pointers[i] = &v
	}
	rows.Scan(pointers...)

	rec := record{raw: make(map[string]reflect.Value, len(cols))}
	for key, value := range container {
		rec.raw[key] = reflect.Indirect(reflect.ValueOf(value)).Elem()
	}
	return &rec
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

	cols, _ := rows.Columns()
	return newRecord(rows, cols), err
}

func (db *DB) QueryRecords(query string, args ...interface{}) ([]Record, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	ret := make([]Record, 0, 10)
	for rows.Next() {
		ret = append(ret, newRecord(rows, cols))
	}
	return ret, err
}
