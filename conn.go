package wz_rpc_go

import (
	"context"
	"errors"
	"net"
	"time"
)

var (
	ErrCallTimeout = errors.New("call timeout")
)

type Conn struct {
	net.Conn
	Codec    Codec
	unusable bool
	p        Pool
}

func (rc *Conn) Call(method string, in, out interface{}) error {

	err := rc.Codec.Send(method, in, "")
	if err != nil {
		return err
	}

	err = rc.Codec.Recv(nil, out, nil)
	if err != nil {
		return err
	}

	return nil
}

func (rc *Conn) CallWithTimeout(method string, in, out interface{}, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- rc.Call(method, in, out)
	}()

	for {
		select {
		case <-ctx.Done():
			rc.MarkUnusable()
			return ErrCallTimeout
		case err := <-errChan:
			return err
		}
	}

}

func (rc *Conn) Close() error {
	if rc.p == nil || rc.unusable {
		return rc.Conn.Close()
	}

	rc.p.Put(rc)
	return nil
}

func (rc *Conn) MarkUnusable() {
	rc.unusable = true
	return
}
