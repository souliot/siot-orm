package pool

import (
	"errors"
	"sync"
)

var (
	pools = &_pools{cache: make(map[string]Pool)}
	//ErrClosed 连接池已经关闭Error
	ErrClosed        = errors.New("pool is closed")
	ErrRegisterPool  = errors.New("register pool error")
	ErrReleasePool   = errors.New("release pool error")
	ErrGetConnection = errors.New("get connection error")
	ErrPutConnection = errors.New("put connection error")
)

// Pool 基本方法
type Pool interface {
	Get() (interface{}, error)

	Put(interface{}) error

	Close(interface{}) error

	Release()

	Len() int
}

type _pools struct {
	mux   sync.RWMutex
	cache map[string]Pool
}

// add pool with pool name.
func (m *_pools) add(name string, p Pool, force bool) (added bool) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if force {
		if _, ok := m.cache[name]; ok {
			m.cache[name].Release()
		}
		m.cache[name] = p
		added = true
		return
	}
	if _, ok := m.cache[name]; !ok {
		m.cache[name] = p
		added = true
	}
	return
}

// get pool if cached.
func (m *_pools) get(name string) (p Pool, ok bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()
	p, ok = m.cache[name]
	return
}

func GetClient(poolName string) (c interface{}, err error) {
	if p, ok := pools.get(poolName); ok {
		v, err := p.Get()
		if err == nil {
			c = v
			defer PutClient(poolName, c)
		}
		return c, err
	}
	return nil, ErrGetConnection
}

func PutClient(poolName string, c interface{}) (err error) {
	if p, ok := pools.get(poolName); ok {
		err = p.Put(c)
		return
	}
	return ErrPutConnection
}

func ReleasePool(poolName string) (err error) {
	if p, ok := pools.get(poolName); ok {
		p.Release()
		return nil
	}
	return ErrReleasePool
}
