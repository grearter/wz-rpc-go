package wz_rpc_go

import (
	"encoding/json"
	"io"
	"net"
)

type CodecFactory func(conn net.Conn) Codec

type Codec interface {
	Send(method string, in interface{}, strErr string) error
	Recv(method *string, out interface{}, strErr *string) error
	ParseRaw(raw []byte, out interface{}) error
}

type JsonCodec struct {
	Enc *json.Encoder
	Dec *json.Decoder
}

func (jc *JsonCodec) Send(method string, in interface{}, strErr string) error {
	req := &rpcMessage{
		Method:  method,
		Content: in,
		Err:     strErr,
	}

	return jc.Enc.Encode(req)
}

func (jc *JsonCodec) Recv(method *string, out interface{}, strErr *string) error {

	var msg struct {
		Method  string          `json:"method"`
		Content json.RawMessage `json:"content"`
		Err     string          `json:"err"`
	}

	err := jc.Dec.Decode(&msg)
	if err != nil {
		return err
	}

	if method != nil {
		*method = msg.Method
	}

	if _, ok := out.(*[]byte); ok {
		*(out.(*[]byte)), _ = msg.Content.MarshalJSON()
	} else {
		err = json.Unmarshal(msg.Content, out)
		if err != nil {
			return err
		}
	}

	if msg.Err != "" && strErr != nil {
		*strErr = msg.Err
	}

	return nil
}

func (jc *JsonCodec) ParseRaw(raw []byte, out interface{}) error {
	if _, ok := out.(*[]byte); ok {
		*(out.(*[]byte)) = raw
		return nil
	}

	return json.Unmarshal(raw, out)
}

func NewJsonCodec(conn net.Conn) Codec {
	return &JsonCodec{
		Enc: json.NewEncoder(conn.(io.Writer)),
		Dec: json.NewDecoder(conn.(io.Reader)),
	}
}
