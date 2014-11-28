package goshine

import (
	"errors"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"net"
	"strconv"
	"time"
)

type GS_STATUS int

const (
	GS_STATUS_DISCONNECTED GS_STATUS = 0
	GS_STATUS_CONNECTED    GS_STATUS = 1
	GS_STATUS_ERROR        GS_STATUS = 2
)

var GS_TYPE_MAP = map[string]string{
	"TTypeId_BOOLEAN_TYPE":      "STRING",
	"TTypeId_TINYINT_TYPE":      "TINYINT",
	"TTypeId_SMALLINT_TYPE":     "SMALLINT",
	"TTypeId_INT_TYPE":          "INT",
	"TTypeId_BIGINT_TYPE":       "BIGINT",
	"TTypeId_FLOAT_TYPE":        "FLOAT",
	"TTypeId_DOUBLE_TYPE":       "DOUBLE",
	"TTypeId_STRING_TYPE":       "STRING",
	"TTypeId_TIMESTAMP_TYPE":    "TIMESTAMP",
	"TTypeId_BINARY_TYPE":       "BINARY",
	"TTypeId_ARRAY_TYPE":        "ARRAY",
	"TTypeId_MAP_TYPE":          "MAP",
	"TTypeId_STRUCT_TYPE":       "STRUCT",
	"TTypeId_UNION_TYPE":        "UNION",
	"TTypeId_USER_DEFINED_TYPE": "USER_DEFINED",
	"TTypeId_DECIMAL_TYPE":      "DECIMAL",
	"TTypeId_NULL_TYPE":         "NULL",
	"TTypeId_DATE_TYPE":         "DATE",
	"TTypeId_VARCHAR_TYPE":      "VARCHAR",
	"TTypeId_CHAR_TYPE":         "CHAR",
}

const BUFFER_SIZE int = 256 * 1024

type Goshine struct {
	host            string
	port            int
	username        string
	password        string
	database        string
	client          *TCLIServiceClient
	session         *TSessionHandle
	operationHandle *TOperationHandle
	status          GS_STATUS
}

type GsFieldInfo struct {
	Name    string
	Type    string
	Comment string
}

type GsResultSet struct {
	Data   [][]string
	Schema []GsFieldInfo
}

func NewGoshine(host string, port int, username string, password string, database string) *Goshine {
	return &Goshine{host: host, port: port, username: username, password: password, status: GS_STATUS_DISCONNECTED, database: database}
}

func (s *Goshine) GetStatus() GS_STATUS {
	return s.status
}

func (s *Goshine) Connect() error {
	if s.status == GS_STATUS_CONNECTED {
		return nil
	}
	//init thrift transport
	socket, err := thrift.NewTSocket(net.JoinHostPort(s.host, strconv.Itoa(s.port)))
	if err != nil {
		errLog.Printf("error resolving address: %s:%d err: %s\n", s.host, s.port, err)
		return errors.New("address resolve failed")
	}

	transport := thrift.NewTBufferedTransport(socket, BUFFER_SIZE)
	if err := transport.Open(); err != nil {
		errLog.Printf("error connecting to %s:%d err: %s\n", s.host, s.port, err)
		return errors.New("connect failed")
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := NewTCLIServiceClientFactory(transport, protocolFactory)

	//open session!
	openSessionReq := NewTOpenSessionReq()
	openSessionReq.Username = &s.username
	openSessionReq.Password = &s.password
	ret, err := client.OpenSession(openSessionReq)
	if err != nil {
		errLog.Printf(
			"error opening session to %s:%d err: %+v",
			s.host, s.port, err)
		transport.Close()
		return errors.New("open session failed")
	}

	s.session = ret.SessionHandle
	s.client = client
	s.status = GS_STATUS_CONNECTED

	return nil
}

func (s *Goshine) reConnect() error {
	for i := 0; i < 3; i++ {
		err := s.Connect()
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Second)
	}
	s.status = GS_STATUS_ERROR
	return errors.New("error reconnecting")
}

func (s *Goshine) ensureConnected() {
	sql := fmt.Sprintf("use %s", s.database)
	err := s.execute(sql)
	if err != nil {
		s.Close()
		if e := s.reConnect(); e != nil {
			panic(fmt.Sprintf("lost connection with spark server: %s:%d\n", s.host, s.port))
		}
	}
}

func (s *Goshine) Close() error {
	if s.status == GS_STATUS_DISCONNECTED {
		return nil
	}

	s.status = GS_STATUS_DISCONNECTED

	closeSessionReq := NewTCloseSessionReq()
	closeSessionReq.SessionHandle = s.session

	ret, err := s.client.CloseSession(closeSessionReq)
	if err != nil {
		return errors.New(fmt.Sprintf("close session fail, %s", err))
	}
	if ret.Status.StatusCode != TStatusCode_SUCCESS_STATUS {
		return errors.New(
			fmt.Sprintf("close session fail, spark error: % msg: %s",
				ret.Status.ErrorCode, *ret.Status.ErrorMessage))
	}

	s.status = GS_STATUS_DISCONNECTED
	return nil
}

func (s *Goshine) Execute(sql string) error {
	s.ensureConnected()
	return s.execute(sql)
}

func (s *Goshine) execute(sql string) error {
	query := NewTExecuteStatementReq()
	query.SessionHandle = s.session
	query.Statement = sql
	query.ConfOverlay = make(map[string]string)

	ret, err := s.client.ExecuteStatement(query)
	if err != nil {
		return errors.New(fmt.Sprintf("execution fail, %s", err))
	}
	if ret.Status.StatusCode != TStatusCode_SUCCESS_STATUS {
		return errors.New(
			fmt.Sprintf("Execute fail, spark error: %d msg: %s",
				*ret.Status.ErrorCode, *ret.Status.ErrorMessage))
	}

	s.operationHandle = ret.OperationHandle

	return nil
}

func (s *Goshine) getValueStr(colval *TColumnValue) string {
	if colval.IsSetBoolVal() {
		if *colval.GetBoolVal().Value == true {
			return "True"
		} else {
			return "False"
		}
	}

	if colval.IsSetByteVal() {
		return strconv.FormatInt(int64(*colval.GetByteVal().Value), 10)
	}

	if colval.IsSetI16Val() {
		return strconv.FormatInt(int64(*colval.GetI16Val().Value), 10)
	}

	if colval.IsSetI32Val() {
		return strconv.FormatInt(int64(*colval.GetI32Val().Value), 10)
	}

	if colval.IsSetI64Val() {
		return strconv.FormatInt(int64(*colval.GetI64Val().Value), 10)
	}

	if colval.IsSetDoubleVal() {
		return strconv.FormatFloat(*colval.GetDoubleVal().Value, 'f', -1, 64)
	}

	if colval.IsSetStringVal() {
		if colval.GetStringVal().Value == nil {
			return ""
		} else {
			return *colval.GetStringVal().Value
		}
	}

	return ""
}

func (s *Goshine) FetchAll(sql string) (*GsResultSet, error) {

	if s.status != GS_STATUS_CONNECTED {
		return nil, errors.New("not connected")
	}

	if err := s.Execute(sql); err != nil {
		return nil, err
	}

	fetchReq := NewTFetchResultsReq()
	fetchReq.OperationHandle = s.operationHandle
	fetchReq.Orientation = TFetchOrientation_FETCH_NEXT
	fetchReq.MaxRows = 10000

	ret, err := s.client.FetchResults(fetchReq)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("FetchResults fail, err: %x msg: %s",
			ret.Status.ErrorCode, ret.Status.ErrorMessage))
	}

	results := [][]string{}
	for _, record := range ret.Results.Rows {
		row := []string{}
		for _, col := range record.ColVals {
			strval := s.getValueStr(col)
			row = append(row, strval)
		}
		results = append(results, row)
	}

	meta, err := s.getResultSetMetadata()

	resultSet := &GsResultSet{Data: results, Schema: meta}

	return resultSet, nil
}

func (s *Goshine) getResultSetMetadata() ([]GsFieldInfo, error) {
	if s.status != GS_STATUS_CONNECTED {
		return nil, errors.New("not connected")
	}

	if s.operationHandle == nil {
		return nil, errors.New("invalid OperationHandle, try make a query first")
	}

	req := NewTGetResultSetMetadataReq()
	req.OperationHandle = s.operationHandle

	ret, err := s.client.GetResultSetMetadata(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("GetResultSetMetadata fail, %s", err))
	}
	if ret.Status.StatusCode != TStatusCode_SUCCESS_STATUS {
		return nil, errors.New(
			fmt.Sprintf("GetResultSetMetadata fail, spark error: %x msg: %s",
				ret.Status.ErrorCode, *ret.Status.ErrorMessage))
	}

	results := []GsFieldInfo{}
	for _, column := range ret.Schema.Columns {
		typestr, ok := GS_TYPE_MAP[column.TypeDesc.Types[0].PrimitiveEntry.TypeA1.String()]
		if !ok {
			typestr = "UNKNOWN"
		}
		row := GsFieldInfo{Name: column.ColumnName, Type: typestr, Comment: *column.Comment}
		results = append(results, row)
	}

	return results, nil
}
