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

func (r *record) Model(rawVal interface{}) error {
	if r == nil {
		return nil
	}
	r.m.RLock()
	defer r.m.RUnlock()

	return newModel(r.raw, rawVal, func(in, out reflect.Type, data interface{}) (interface{}, error) {
        dataMap, ok := data.(map[string]reflect.Value)
        if !ok {
            return data, nil
        }
        ret := make(map[string]interface{}, len(dataMap))
        for key, val := range dataMap {
            ret[key] = val.Interface()
        }
        return ret, nil
    })
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
    container := newContainer(rows, cols)
	return newModel(container, model, func(in, out reflect.Type, data interface{}) (interface{}, error) {
        dataMap, ok := data.(map[string]interface{})
        if !ok {
            return data, nil
        }
        for key, val := range dataMap {
            //dataMap[key] = *(*interface{})(&val)
            dataMap[key] = reflect.ValueOf(val).Elem().Interface()
        }
        return dataMap, nil
    })
}

func (db *DB) QueryRecords(query string, args ...interface{}) ([]Record, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := make([]Record, 0, 10)
	cols, err := rows.Columns()
	if err != nil {
		return ret, err
	}
	for rows.Next() {
		ret = append(ret, newRecord(rows, cols))
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
	return newRecord(rows, cols), err
}

func normalizeValue(value reflect.Value) reflect.Value {
	k := value.Kind()
	if k == reflect.Interface || k == reflect.Ptr {
		return normalizeValue(value.Elem())
	}
	return value
}

func newContainer(rows *sql.Rows, cols []string) map[string]interface{} {
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
	rows.Scan(pointers...)
    return container
}

func newRecord(rows *sql.Rows, cols []string) Record {
    container := newContainer(rows, cols)
	rec := record{raw: make(map[string]reflect.Value, len(cols))}
	for key, value := range container {
		rec.raw[key] = reflect.Indirect(reflect.ValueOf(value)).Elem()
	}
	return &rec
}

func newModel(src, dst interface{}, decodeHook mapstructure.DecodeHookFuncType) error {
	config := &mapstructure.DecoderConfig{
        DecodeHook:       decodeHook,
		Metadata:         nil,
		Result:           dst,
		WeaklyTypedInput: true,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(src)
}
