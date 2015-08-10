package spcdb

import (
	"fmt"
	"sync"
	"time"
)

var (
	MaxConnsInPool    int           = 20
	TimeoutPing       time.Duration = 2 // in minutes
	DefaultDriverName string        = "postgres"
)

type DBConfiguer interface {
	DriverName() string
	IsPing()     bool
	String()     string
}

type poolType struct {
	conns  []*DB
	busy   []bool
	driver string
	dsn    string
	ping   bool
	m      sync.RWMutex
}

type ptrType struct {
	index    int
	nameConn string
}

var pools map[string]*poolType
var poolPtr map[*DB]ptrType
var mPtr sync.RWMutex

func init() {
	pools = make(map[string]*poolType, 10)
	poolPtr = make(map[*DB]ptrType, 40)

	go func() {
		for {
			<-time.After(TimeoutPing * time.Minute)
			for _, pool := range pools {
				if pool.ping {
					pingPool(pool)
				}
			}
		}
	}()
}

func pingPool(pool *poolType) {
	freePools := make([]bool, MaxConnsInPool)
	pool.m.RLock()
	copy(freePools, pool.busy)
	pool.m.RUnlock()
	for index, busy := range freePools {
		if !busy {
			if db := pool.conns[index]; db != nil {
				db.Ping()
			}
		}
	}
}

func (db *DB) ConnectionName() string {
	mPtr.RLock()
	defer mPtr.RUnlock()
	if ptr, found := poolPtr[db]; found {
		return ptr.nameConn
	}
	return ""
}

func (db *DB) ReturnToPool() bool {
	return ReturnToPool(db)
}

func NewPoolConnection(connectionName string, cfg DBConfiguer) {
	drvName := cfg.DriverName()
	if drvName == "" {
		drvName = DefaultDriverName
	}
	pools[connectionName] = &poolType{
		conns:  make([]*DB, MaxConnsInPool),
		busy:   make([]bool, MaxConnsInPool),
		driver: drvName,
		dsn:    cfg.String(),
		ping:   cfg.IsPing(),
	}
}

func GetFromPool(connectionName string) (*DB, error) {
	pool, found := pools[connectionName]
	if !found {
		return nil, fmt.Errorf("spcdb: No DB connection by name '%s'", connectionName)
	}

	pool.m.Lock()
	defer pool.m.Unlock()
	for index, busy := range pool.busy {
		if !busy {
			if db := pool.conns[index]; db != nil {
				pool.busy[index] = true
				return db, nil
			}
			db, err := Open(pool.driver, pool.dsn)
			if err == nil {
				pool.conns[index] = db
				mPtr.Lock()
				poolPtr[db] = ptrType{index, connectionName}
				mPtr.Unlock()
				pool.busy[index] = true
			}
			return db, err
		}
	}
	return nil, fmt.Errorf("spcdb: No idle DB connections; '%s'", connectionName)
}

func ReturnToPool(db *DB) bool {
	mPtr.RLock()
	ptr, found := poolPtr[db]
	mPtr.RUnlock()
	if !found {
		return false
	}
	pool, found := pools[ptr.nameConn]
	if !found {
		return false
	}
	pool.m.Lock()
	pool.busy[ptr.index] = false
	pool.m.Unlock()
	return true
}
