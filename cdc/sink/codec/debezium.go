package codec

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// AvroEventBatchEncoder converts the events to binary Avro data
type DebeziumAvroEventBatchEncoder struct {
	*AvroEventBatchEncoder
	serverName string
}

// creates an DebeziumAvroEventBatchEncoder
func NewDebeziumAvroEventBatchEncoder() EventBatchEncoder {
	return &DebeziumAvroEventBatchEncoder{
		AvroEventBatchEncoder: &AvroEventBatchEncoder{
			valueSchemaManager: nil,
			keySchemaManager:   nil,
			resultBuf:          make([]*MQMessage, 0, 4096),
		},
		serverName: "",
	}
}

func (d *DebeziumAvroEventBatchEncoder) SetServerName(serverName string) {
	d.serverName = serverName
}

func (d *DebeziumAvroEventBatchEncoder) GetServerName() string {
	return d.serverName
}

// AppendRowChangedEvent appends a row changed event into the batch
func (d *DebeziumAvroEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	table := e.Table
	tableVersion := e.TableInfoVersion
	serverName := d.serverName
	keySchemaManager := d.keySchemaManager
	valueSchemaManager := d.valueSchemaManager
	if e.IsDelete() {
		result, err := convertToDbzEvent(e)
		if err != nil {
			return EncoderNoOperation, err
		}

		msg, err := buildDeleteMsg(result, table, tableVersion, serverName, e, keySchemaManager, valueSchemaManager)
		if err != nil {
			return EncoderNoOperation, err
		}

		d.resultBuf = append(d.resultBuf, msg)
	} else if e.PreColumns == nil {
		result, err := convertToDbzEvent(e)
		if err != nil {
			return EncoderNoOperation, err
		}

		msg, err := buildInsertMsg(result, table, tableVersion, serverName, e, keySchemaManager, valueSchemaManager)
		if err != nil {
			return EncoderNoOperation, err
		}

		d.resultBuf = append(d.resultBuf, msg)
	} else {
		result, err := convertToDbzEvent(e)
		if err != nil {
			return EncoderNoOperation, err
		}
		// we will handle pk update as delete + insert
		if result.pkUpdate {
			// this is a delete with previous values
			msg, err := buildDeleteMsg(result, table, tableVersion, serverName, e, keySchemaManager, valueSchemaManager)
			if err != nil {
				return EncoderNoOperation, err
			}

			d.resultBuf = append(d.resultBuf, msg)
			// this is an insert with current values
			msg, err = buildInsertMsg(result, table, tableVersion, serverName, e, keySchemaManager, valueSchemaManager)
			if err != nil {
				return EncoderNoOperation, err
			}

			d.resultBuf = append(d.resultBuf, msg)
		} else {
			msg := NewMQMessage(ProtocolDebeziumAvro, nil, nil, e.CommitTs, model.MqMessageTypeRow, &e.Table.Schema, &e.Table.Table)

			keys, err := buildMsgKey(result.pkCols, result.keys, table, tableVersion, serverName, e, keySchemaManager)
			if err != nil {
				return EncoderNoOperation, err
			}
			msg.Key = keys

			evlp := newEnvelope(table, serverName, e)
			avroKey := serverName + "." + table.Schema + "." + table.Table + ".Value"
			evlp["op"] = "u"
			evlp["before"] = map[string]interface{}{avroKey: result.preValues}
			evlp["after"] = map[string]interface{}{avroKey: result.values}

			values, err := buildMsgValue(evlp, table, tableVersion, serverName, e, valueSchemaManager)
			msg.Value = values

			d.resultBuf = append(d.resultBuf, msg)
		}
	}
	return EncoderNeedAsyncWrite, nil
}

type dbzEvent struct {
	keys      map[string]interface{}
	values    map[string]interface{}
	preKeys   map[string]interface{}
	preValues map[string]interface{}
	pkCols    []*model.Column
	prePkCols []*model.Column
	pkUpdate  bool
}

func convertToDbzEvent(e *model.RowChangedEvent) (*dbzEvent, error) {
	if e.IsDelete() {
		return rowToDbzEvent(e.PreColumns, true)
	} else if e.PreColumns == nil {
		return rowToDbzEvent(e.Columns, false)
	} else {
		pre, err := rowToDbzEvent(e.PreColumns, true)
		if err != nil {
			return nil, err
		}
		result, err := rowToDbzEvent(e.Columns, false)
		if err != nil {
			return nil, err
		}
		// check pk update
		var pkUpdate bool = false
		if len(result.keys) != len(pre.preKeys) {
			pkUpdate = true
		} else {
			for keyName, keyVal := range result.keys {
				preKeyVal := pre.preKeys[keyName]
				if !reflect.DeepEqual(keyVal, preKeyVal) {
					pkUpdate = true
					break
				}
			}
		}
		return &dbzEvent{
			keys:      result.keys,
			values:    result.values,
			preKeys:   pre.preKeys,
			preValues: pre.preValues,
			pkCols:    result.pkCols,
			prePkCols: pre.prePkCols,
			pkUpdate:  pkUpdate,
		}, nil
	}
}

type dbzAvroSchema struct {
	Type      string                   `json:"type"`
	Name      string                   `json:"name"`
	Namespace string                   `json:"namespace,omitempty"`
	Fields    []map[string]interface{} `json:"fields"`
}

type dbzAvroLogicalType struct {
	Type    string      `json:"type"`
	Name    string      `json:"connect.name"`
	Default interface{} `json:"connect.default,omitempty"`
}

func getDbzAvroDataTypeFromColumn(col *model.Column) (interface{}, error) {
	log.Info("DEBUG: getAvroDataTypeFromColumn", zap.Reflect("col", col))
	switch col.Type {
	case mysql.TypeFloat:
		return "float", nil
	case mysql.TypeDouble:
		return "double", nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		return "string", nil
	case mysql.TypeDate, mysql.TypeNewDate:
		return dbzAvroLogicalType{
			Type: "int",
			Name: "io.debezium.time.Date",
		}, nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		return dbzAvroLogicalType{
			Type: "long",
			Name: "io.debezium.time.Timestamp",
		}, nil
	case mysql.TypeDuration:
		return dbzAvroLogicalType{
			Type: "long",
			Name: "io.debezium.time.MicroTime",
		}, nil
	case mysql.TypeEnum:
		return unsignedLongAvroType, nil
	case mysql.TypeSet:
		return unsignedLongAvroType, nil
	case mysql.TypeBit:
		return "boolean", nil
	case mysql.TypeNewDecimal:
		return "string", nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return "int", nil
	case mysql.TypeLong:
		if col.Flag.IsUnsigned() {
			return "long", nil
		}
		return "int", nil
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			return unsignedLongAvroType, nil
		}
		return "long", nil
	case mysql.TypeNull:
		return "null", nil
	case mysql.TypeJSON:
		return "string", nil
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return "bytes", nil
	case mysql.TypeYear:
		return "long", nil
	default:
		log.Fatal("Unknown MySql type", zap.Reflect("mysql-type", col.Type))
		return "", errors.New("Unknown Mysql type")
	}
}

func columnToDbzAvroNativeData(col *model.Column) (interface{}, string, error) {
	if col.Value == nil {
		return nil, "null", nil
	}

	handleUnsignedInt64 := func() (interface{}, string, error) {
		var retVal interface{}
		switch v := col.Value.(type) {
		case uint64:
			retVal = big.NewRat(0, 1).SetUint64(v)
		case int64:
			retVal = big.NewRat(0, 1).SetInt64(v)
		}
		return retVal, string("bytes." + decimalType), nil
	}

	switch col.Type {
	case mysql.TypeDate, mysql.TypeNewDate:
		str := col.Value.(string)
		if str == types.ZeroDateStr {
			return nil, "null", nil
		}
		t, err := time.Parse(types.DateFormat, str)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return t.Unix() / 86400, "int", nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		str := col.Value.(string)
		if str == types.ZeroDatetimeStr {
			return nil, "null", nil
		}
		t, err := time.Parse(types.TimeFormat, str)
		if err == nil {
			return t.UTC().UnixNano() / 1000000, "long", nil
		}

		t, err = time.Parse(types.TimeFSPFormat, str)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return t.UTC().UnixNano() / 1000000, "long", nil
	case mysql.TypeDuration:
		str := col.Value.(string)
		var (
			hours   int
			minutes int
			seconds int
			frac    string
		)
		_, err := fmt.Sscanf(str, "%d:%d:%d.%s", &hours, &minutes, &seconds, &frac)
		if err != nil {
			_, err := fmt.Sscanf(str, "%d:%d:%d", &hours, &minutes, &seconds)
			frac = "0"

			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
		}

		fsp := len(frac)
		fracInt, err := strconv.ParseInt(frac, 10, 32)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		fracInt = int64(float64(fracInt) * math.Pow10(6-fsp))

		d := types.NewDuration(hours, minutes, seconds, int(fracInt), int8(fsp)).Duration
		return d.Microseconds(), "long", nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if col.Flag.IsBinary() {
			switch val := col.Value.(type) {
			case string:
				return []byte(val), "bytes", nil
			case []byte:
				return val, "bytes", nil
			}
		} else {
			switch val := col.Value.(type) {
			case string:
				return val, "string", nil
			case []byte:
				return string(val), "string", nil
			}
		}
		log.Fatal("Avro could not process text-like type", zap.Reflect("col", col))
		return nil, "", errors.New("Unknown datum type")
	case mysql.TypeYear:
		return col.Value.(int64), "long", nil
	case mysql.TypeJSON:
		return col.Value.(string), "string", nil
	case mysql.TypeNewDecimal:
		return col.Value.(string), "string", nil
	case mysql.TypeEnum:
		return handleUnsignedInt64()
	case mysql.TypeSet:
		return handleUnsignedInt64()
	case mysql.TypeBit:
		v := col.Value.(uint64)
		if v == 0 {
			return false, "boolean", nil
		} else if v == 1 {
			return true, "boolean", nil
		}
		return nil, "", errors.New("Unknown datum type")
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLong:
		if col.Flag.IsUnsigned() {
			return int64(col.Value.(uint64)), "long", nil
		}
		return col.Value.(int64), "int", nil
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			return handleUnsignedInt64()
		}
		return col.Value.(int64), "long", nil
	default:
		avroType, err := getAvroDataTypeFallback(col.Value)
		if err != nil {
			return nil, "", err
		}
		return col.Value, avroType, nil
	}
}

// ColumnInfoToAvroSchema generates the Avro schema JSON for the corresponding columns
func toKeySchema(table *model.TableName, serverName string, columnInfo []*model.Column) (string, error) {
	var namespace = serverName + "." + table.Schema + "." + table.Table
	top := dbzAvroSchema{
		Type:      "record",
		Name:      "Key",
		Namespace: namespace,
		Fields:    nil,
		// ConnectName: namespace + ".Key",
	}

	for _, col := range columnInfo {
		avroType, err := getDbzAvroDataTypeFromColumn(col)
		if err != nil {
			return "", err
		}
		field := map[string]interface{}{"name": col.Name, "type": avroType}
		top.Fields = append(top.Fields, field)
	}

	str, err := json.Marshal(&top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Debug("Debezium Key Schema JSON generated", zap.ByteString("schema", str))
	return string(str), nil
}

func toValueSchema(table *model.TableName, serverName string, e *model.RowChangedEvent) (string, error) {
	var namespace = serverName + "." + table.Schema + "." + table.Table
	values := dbzAvroSchema{
		Type:      "record",
		Name:      "Value",
		Namespace: namespace,
		Fields:    nil,
		// ConnectName: namespace + ".Value",
	}
	cols := e.Columns
	if len(cols) == 0 {
		cols = e.PreColumns
	}
	for _, col := range cols {
		if col == nil {
			// TODO delete不能获取到全字段
			continue
		}
		avroType, err := getDbzAvroDataTypeFromColumn(col)
		if err != nil {
			return "", err
		}
		field := make(map[string]interface{})
		field["name"] = col.Name
		if col.Flag.IsHandleKey() {
			field["type"] = avroType
		} else {
			field["type"] = []interface{}{"null", avroType}
			field["default"] = nil
		}

		values.Fields = append(values.Fields, field)
	}

	sourceFields := []map[string]interface{}{
		{"name": "version", "type": "string"},
		{"name": "connector", "type": "string"},
		// server name
		{"name": "name", "type": "string"},
		{"name": "ts_ms", "type": "long"},
		{"name": "snapshot", "type": []interface{}{"null", dbzAvroLogicalType{"string", "io.debezium.data.Enum", "false"}}},
		{"name": "db", "type": "string"},
		{"name": "table", "type": "string"},
	}
	source := dbzAvroSchema{
		Type:      "record",
		Name:      "Source",
		Namespace: "io.debezium.connector.mysql",
		Fields:    sourceFields,
	}

	top := dbzAvroSchema{
		Type:      "record",
		Name:      "Envelope",
		Namespace: namespace,
		Fields:    nil,
	}

	before := make(map[string]interface{})
	before["name"] = "before"
	before["type"] = []interface{}{"null", values}
	before["default"] = nil

	after := make(map[string]interface{})
	after["name"] = "after"
	after["type"] = []interface{}{"null", "Value"}
	after["default"] = nil

	top.Fields = []map[string]interface{}{
		before,
		after,
		{"name": "source", "type": source},
		{"name": "op", "type": "string"},
		{"name": "ts_ms", "type": []interface{}{"null", "long"}, "default": "null"},
	}
	str, err := json.Marshal(&top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Debug("Debezium Value Schema JSON generated", zap.ByteString("schema", str))
	return string(str), nil
}

func newEnvelope(table *model.TableName, serverName string, e *model.RowChangedEvent) map[string]interface{} {
	evlp := make(map[string]interface{}, 5)
	evlp["source"] = map[string]interface{}{
		"version":   "1.2.2.Final",
		"connector": "mysql",
		"name":      serverName,
		"ts_ms":     int64(e.CommitTs >> 18),
		"snapshot":  map[string]interface{}{"string": "false"},
		"db":        table.Schema,
		"table":     table.Table,
	}
	evlp["ts_ms"] = map[string]interface{}{"long": time.Now().UnixNano() / 1e6}
	return evlp
}

func buildMsgKey(pkCols []*model.Column, keys map[string]interface{}, table *model.TableName, tableVersion uint64, serverName string, e *model.RowChangedEvent, manager *AvroSchemaManager) ([]byte, error) {
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("Debezium Native Key generated", zap.Reflect("keys", keys))
	}
	schemaGen := func() (string, error) {
		schema, err := toKeySchema(table, serverName, pkCols)
		if err != nil {
			return "", errors.Annotate(err, "DebeziumAvroEventBatchEncoder: generating keys schema failed")
		}
		return schema, nil
	}
	codec, schemaId, err := manager.GetCachedOrRegister(context.Background(), *table, tableVersion, schemaGen)
	if err != nil {
		return nil, errors.Annotate(err, "DebeziumAvroEventBatchEncoder: get-or-register failed")
	}
	bin, err := codec.BinaryFromNative(nil, keys)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroEncodeToBinary, err), "AvroEventBatchEncoder: converting to Avro binary failed")
	}
	buf := new(bytes.Buffer)
	data := []interface{}{magicByte, int32(schemaId), bin}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}

func buildMsgValue(values map[string]interface{}, table *model.TableName, tableVersion uint64, serverName string, e *model.RowChangedEvent, manager *AvroSchemaManager) ([]byte, error) {
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("Debezium Native Value generated", zap.Reflect("values", values))
	}
	schemaGen := func() (string, error) {
		schema, err := toValueSchema(table, serverName, e)
		if err != nil {
			return "", errors.Annotate(err, "DebeziumAvroEventBatchEncoder: generating values schema failed")
		}
		return schema, nil
	}
	codec, schemaId, err := manager.GetCachedOrRegister(context.Background(), *table, tableVersion, schemaGen)
	if err != nil {
		return nil, errors.Annotate(err, "DebeziumAvroEventBatchEncoder: get-or-register failed")
	}
	bin, err := codec.BinaryFromNative(nil, values)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroEncodeToBinary, err), "AvroEventBatchEncoder: converting to Avro binary failed")
	}
	buf := new(bytes.Buffer)
	data := []interface{}{magicByte, int32(schemaId), bin}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}

func buildDeleteMsg(result *dbzEvent, table *model.TableName, tableVersion uint64, serverName string, e *model.RowChangedEvent,
	keySchemaManager *AvroSchemaManager, valueSchemaManager *AvroSchemaManager) (*MQMessage, error) {
	msg := NewMQMessage(ProtocolDebeziumAvro, nil, nil, e.CommitTs, model.MqMessageTypeRow, &e.Table.Schema, &e.Table.Table)
	keys, err := buildMsgKey(result.prePkCols, result.preKeys, table, tableVersion, serverName, e, keySchemaManager)
	if err != nil {
		return nil, err
	}
	msg.Key = keys

	evlp := newEnvelope(table, serverName, e)
	avroKey := serverName + "." + table.Schema + "." + table.Table + ".Value"
	evlp["op"] = "d"
	evlp["before"] = map[string]interface{}{avroKey: result.preValues}
	evlp["after"] = nil

	values, err := buildMsgValue(evlp, table, tableVersion, serverName, e, valueSchemaManager)
	msg.Value = values
	return msg, nil
}

func buildInsertMsg(result *dbzEvent, table *model.TableName, tableVersion uint64, serverName string, e *model.RowChangedEvent,
	keySchemaManager *AvroSchemaManager, valueSchemaManager *AvroSchemaManager) (*MQMessage, error) {
	msg := NewMQMessage(ProtocolDebeziumAvro, nil, nil, e.CommitTs, model.MqMessageTypeRow, &e.Table.Schema, &e.Table.Table)
	keys, err := buildMsgKey(result.pkCols, result.keys, table, tableVersion, serverName, e, keySchemaManager)
	if err != nil {
		return nil, err
	}
	msg.Key = keys

	evlp := newEnvelope(table, serverName, e)
	avroKey := serverName + "." + table.Schema + "." + table.Table + ".Value"
	evlp["op"] = "c"
	evlp["before"] = nil
	evlp["after"] = map[string]interface{}{avroKey: result.values}

	values, err := buildMsgValue(evlp, table, tableVersion, serverName, e, valueSchemaManager)
	if err != nil {
		return nil, err
	}
	msg.Value = values
	return msg, nil
}

func rowToDbzEvent(cols []*model.Column, isPre bool) (*dbzEvent, error) {
	keys := make(map[string]interface{})
	values := make(map[string]interface{}, len(cols))
	pkCols := make([]*model.Column, 0)
	for _, col := range cols {
		if col == nil {
			continue
		}
		data, typ, err := columnToDbzAvroNativeData(col)
		if err != nil {
			return nil, errors.Annotate(err, "DebeziumAvroEventBatchEncoder: converting to native failed")
		}
		if col.Flag.IsHandleKey() {
			keys[col.Name] = data
			values[col.Name] = data
			pkCols = append(pkCols, col)
			continue
		}
		values[col.Name] = map[string]interface{}{typ: data}
	}
	if isPre {
		return &dbzEvent{
			preKeys:   keys,
			preValues: values,
			prePkCols: pkCols,
			pkUpdate:  false,
		}, nil
	} else {
		return &dbzEvent{
			keys:     keys,
			values:   values,
			pkCols:   pkCols,
			pkUpdate: false,
		}, nil
	}
}
