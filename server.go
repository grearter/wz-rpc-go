package wz_rpc_go

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	typeOfError              = reflect.TypeOf((*error)(nil)).Elem()
	NoExportedMethod         = errors.New("no exported Method")
	ErrNilRequestMethod      = errors.New("nil request Method")
	ErrRequestMethodNotFound = errors.New("request Method not found")
)

type rpcMessage struct {
	Method  string      `json:"method"`
	Content interface{} `json:"content"`
	Err     string      `json:"err,omitempty"`
}

type connection struct {
	c     net.Conn
	s     *Server
	codec Codec
}

func (c *connection) serve() {
	defer c.c.Close()

	var err error

	for {

		var method string
		var para []byte
		err = c.codec.Recv(&method, &para, nil)
		if err != nil {
			return
		}

		resp := c.do(method, para)

		err = c.codec.Send(resp.Method, resp.Content, resp.Err)
		if err != nil {
			return
		}
	}
}

func (c *connection) do(method string, para []byte) (resp *rpcMessage) {
	var (
		inParam  reflect.Value
		outParam reflect.Value
	)

	resp = &rpcMessage{Method: method}

	parts := strings.Split(method, ".")
	if len(parts) != 2 {
		resp.Err = fmt.Sprintf("invalid method(%s)", method)
		return resp
	}

	svc, err := c.s.getService(parts[0])
	if err != nil {
		resp.Err = err.Error()
		return
	}

	mthd, err := svc.getMethod(parts[1])
	if err != nil {
		resp.Err = err.Error()
		return
	}

	inParam = reflect.New(mthd.inType)

	err = c.codec.ParseRaw(para, inParam.Interface())
	if err != nil {
		resp.Err = err.Error()
		return
	}

	outParam = reflect.New(mthd.outType.Elem())

	returnValues := mthd.method.Func.Call([]reflect.Value{svc.receiverValue, inParam.Elem(), outParam})

	errInter := returnValues[0].Interface()

	if errInter != nil {
		resp.Err = errInter.(error).Error()
		return
	}

	resp.Content = outParam.Interface()
	return
}

type service struct {
	receiverType  reflect.Type
	receiverValue reflect.Value
	methodMap     map[string]*serviceMethod
}

func (svc *service) getMethod(methodName string) (*serviceMethod, error) {
	svcMethod, ok := svc.methodMap[methodName]

	if !ok {
		return nil, fmt.Errorf("methodName '%s' not exists", methodName)
	}

	return svcMethod, nil
}

type serviceMethod struct {
	method  reflect.Method
	inType  reflect.Type
	outType reflect.Type
}

type Server struct {
	addr         string
	ln           net.Listener
	serviceMap   map[string]*service
	codecFactory CodecFactory
}

func NewServer(addr string, codecFactory CodecFactory) *Server {
	s := &Server{
		addr:         addr,
		serviceMap:   make(map[string]*service),
		codecFactory: codecFactory,
	}

	if codecFactory == nil {
		s.codecFactory = NewJsonCodec
	}

	return s
}

// Is this an exported - upper case - name?
func isExported(name string) bool {
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(r)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

func (s *Server) Register(receiver interface{}) error {
	recvType := reflect.TypeOf(receiver)
	recvValue := reflect.ValueOf(receiver)

	serviceName := reflect.Indirect(recvValue).Type().Name()
	if serviceName == "" {
		return errors.New("invalid service name")
	}

	newService := &service{
		receiverType:  recvType,
		receiverValue: recvValue,
		methodMap:     make(map[string]*serviceMethod),
	}

	for i := 0; i < recvType.NumMethod(); i++ {
		method := recvType.Method(i)
		methodType := method.Type
		methodName := method.Name

		if method.PkgPath != "" {
			continue
		}

		if methodType.NumIn() != 3 {
			continue
		}

		inType := methodType.In(1)

		if !isExportedOrBuiltinType(inType) {
			continue
		}

		outType := methodType.In(2)
		if outType.Kind() != reflect.Ptr {
			continue
		}

		if !isExportedOrBuiltinType(outType) {
			continue
		}

		if methodType.NumOut() != 1 {
			continue
		}

		if methodType.Out(0) != typeOfError {
			continue
		}

		newService.methodMap[methodName] = &serviceMethod{
			method:  method,
			inType:  inType,
			outType: outType,
		}
	}

	if len(newService.methodMap) <= 0 {
		return NoExportedMethod
	}

	if s.serviceMap == nil {
		s.serviceMap = make(map[string]*service)
	}

	s.serviceMap[serviceName] = newService

	return nil
}

func (s *Server) getService(serviceName string) (*service, error) {
	svc, ok := s.serviceMap[serviceName]

	if !ok {
		return nil, fmt.Errorf("serviceName '%s' not exists", serviceName)
	}

	return svc, nil
}

func (s *Server) getMethod(method string) (_service *service, _serviceMethod *serviceMethod, err error) {
	parts := strings.Split(method, ".")

	if len(parts) != 2 {
		err = fmt.Errorf("invalid Method(%s)", method)
		return
	}

	serviceName, serviceMethodName := parts[0], parts[1]

	if _, ok := s.serviceMap[serviceName]; !ok {
		err = fmt.Errorf("service '%s' not found", serviceName)
		return
	}

	_service = s.serviceMap[serviceName]

	if _, ok := _service.methodMap[serviceMethodName]; ok {
		err = fmt.Errorf("serviceMethod '%s' not found In service '%s'", serviceName, serviceMethodName)
		return
	}

	return
}

func (s *Server) Serve() error {

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln

	for {
		rw, err := s.ln.Accept()
		if err != nil {
			return err
		}

		conn := &connection{
			c:     rw,
			s:     s,
			codec: s.codecFactory(rw),
		}

		go conn.serve()
	}
}
