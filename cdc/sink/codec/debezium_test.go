package codec

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	model2 "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/types"
)

type debeziumAvroEncoderSuite struct {
	encoder EventBatchEncoder
}

var _ = check.Suite(&debeziumAvroEncoderSuite{})

func (s *debeziumAvroEncoderSuite) SetUpSuite(c *check.C) {
	startHTTPInterceptForTestingRegistry(c)

	keyManager, err := NewAvroSchemaManager(context.Background(), &security.Credential{}, "http://127.0.0.1:8081", "-key")
	c.Assert(err, check.IsNil)

	valueManager, err := NewAvroSchemaManager(context.Background(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	encoder := NewDebeziumAvroEventBatchEncoder().(*DebeziumAvroEventBatchEncoder)
	encoder.SetKeySchemaManager(keyManager)
	encoder.SetValueSchemaManager(valueManager)
	encoder.SetServerName("poc238")
	s.encoder = encoder
}

func (s *debeziumAvroEncoderSuite) TearDownSuite(c *check.C) {
	stopHTTPInterceptForTestingRegistry()
}

func (s *debeziumAvroEncoderSuite) TestDbzAvroEncode(c *check.C) {
	defer testleak.AfterTest(c)()
	testCaseUpdate := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag, Value: int64(1)},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: int64(255)},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
		},
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag, Value: int64(1)},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: int64(255)},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
		},
	}

	testCaseDdl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test", Table: "person",
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  model2.ActionCreateTable,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := puller.NewMockPullerManager(c, true)
	defer pm.TearDown()
	pm.MustExec(testCaseDdl.Query)
	ddlPlr := pm.CreatePuller(0, []regionspan.ComparableSpan{regionspan.ToComparableSpan(regionspan.GetDDLSpan())})
	go func() {
		err := ddlPlr.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			c.Fail()
		}
	}()

	info := pm.GetTableInfo("test", "person")
	testCaseDdl.TableInfo = new(model.SimpleTableInfo)
	testCaseDdl.TableInfo.Schema = "test"
	testCaseDdl.TableInfo.Table = "person"
	testCaseDdl.TableInfo.ColumnInfo = make([]*model.ColumnInfo, len(info.Columns))
	for i, v := range info.Columns {
		testCaseDdl.TableInfo.ColumnInfo[i] = new(model.ColumnInfo)
		testCaseDdl.TableInfo.ColumnInfo[i].FromTiColumnInfo(v)
	}

	_, err := s.encoder.EncodeDDLEvent(testCaseDdl)
	c.Check(err, check.IsNil)

	_, err = s.encoder.AppendRowChangedEvent(testCaseUpdate)
	c.Check(err, check.IsNil)
}

func (s *debeziumAvroEncoderSuite) TestConversions(c *check.C) {
	str := "0000-00-00"
	t, err := time.Parse(types.DateFormat, str)
	if err != nil {
		panic(err)
	}
	fmt.Println(t)
}
