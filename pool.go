package wz_rpc_go

import (
	"errors"
	"sync"
)

var (
	ErrInvalidCapacity = errors.New("invalid capacity")
	ErrInvalidFactory  = errors.New("invalid factory")
	ErrPoolClosed      = errors.New("pool has closed")
)

type Pool interface {
	Get() (*Conn, error)
	Put(*Conn)
	Len() int
	Close()
}

type Factory func() (*Conn, error)

type ChanPool struct {
	m           sync.RWMutex
	connections chan *Conn
	factoryFunc Factory
}

func NewChanPool(capacity int, factoryFunc Factory) (Pool, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	if factoryFunc == nil {
		return nil, ErrInvalidFactory
	}

	p := &ChanPool{
		connections: make(chan *Conn, capacity),
		factoryFunc: factoryFunc,
	}

	return p, nil
}

func (p *ChanPool) Close() {
	p.m.Lock()
	connections := p.connections
	p.connections = nil
	p.factoryFunc = nil
	p.m.Unlock()

	close(connections)
	for c := range connections {
		_ = c.Close()
	}

	return
}

func (p *ChanPool) Get() (*Conn, error) {
	p.m.RLock()
	connections := p.connections
	factoryFunc := p.factoryFunc
	p.m.RUnlock()

	if connections == nil {
		return nil, ErrPoolClosed
	}

	select {
	case c := <-p.connections:
		return c, nil
	default:
		c, err := factoryFunc()
		if err != nil {
			return nil, err
		}

		c.p = p
		return c, nil
	}
}

func (p *ChanPool) Put(c *Conn) {
	p.m.RLock()
	if p.connections == nil { // pool已关闭
		p.m.RUnlock()
		_ = c.Conn.Close()
		return
	}

	select {
	case p.connections <- c: // 放入连接池
		p.m.RUnlock()
	default: // 连接池已满
		p.m.RUnlock()
		_ = c.Conn.Close()
	}
	return
}

func (p *ChanPool) Len() int {
	p.m.RLock()
	length := len(p.connections)
	p.m.RUnlock()

	return length
}
