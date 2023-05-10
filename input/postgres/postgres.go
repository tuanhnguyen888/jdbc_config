package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
	"jbdc/event"
	"strings"
)

type InputConfig struct {
	Type                string
	ConnectionString    string
	User                string
	Password            string
	Statement           string
	UserColumn          bool
	TrackingColumn      string
	TrackingColumnType  string
	LastRunMetadataPath string
	RecordLastRun       bool
	CleanRun            bool
	PagingEnabled       bool
	FetchSize           int64
	PageSize            int64
	Schedule            string
	//AddFiled []map[string]interface{}
	Codec string

	Client *gorm.DB
}

func InitHandler() (InputConfig, error) {
	conf := InputConfig{
		Type:                "postgrtes",
		ConnectionString:    "host=localhost port=5424 user=user dbname=postgres password=khong123",
		User:                "user",
		Password:            "khong123",
		Statement:           "SELECT * FROM logs ",
		UserColumn:          true,
		TrackingColumn:      "timestamp",
		TrackingColumnType:  "numberic",
		LastRunMetadataPath: "D:\\data\\BaiTap\\JDBC-Plugin\\input\\postgres\\metadata.json",
		RecordLastRun:       true,
		CleanRun:            false,
		PagingEnabled:       true,
		FetchSize:           2,
		PageSize:            2,
		Schedule:            "* * * * *",
		Codec:               "json",
	}

	db, err := gorm.Open("postgres", conf.ConnectionString)
	if err != nil {
		return InputConfig{}, err
	}
	conf.Client = db

	return conf, nil
}

func (i *InputConfig) Execute() error {
	isSqlLastValue := strings.Contains(i.Statement,":sql_last_value")
	var statement string

	//
	if (i.CleanRun && isSqlLastValue) || (!i.CleanRun && !i.RecordLastRun && !i.UserColumn && isSqlLastValue) {
		switch i.TrackingColumnType {
		case "numberic":
			statement = strings.ReplaceAll(statement,":sql_last_value","0")
		case "string":
			statement = strings.ReplaceAll(statement,":sql_last_value","")
		case "timestamps":
			statement = strings.ReplaceAll(statement,"sql_last_value", "1970-01-01T00:00:00Z")
		default:
			return errors.New("tracking_column_type invalid")
		}

		//Todo: thuc hien truy van
		if i.PagingEnabled {
			//Todo: dùng tracking_value và :sql_value để thực hiện theo dõi theo đồng bộ với fetch_size
		}else {
			//Todo: lấy hết dữ liệu trong 1 câu truy vấn
		}


	}

	//
	if (i.CleanRun && isSqlLastValue) || (!i.CleanRun && !i.RecordLastRun && !i.UserColumn && isSqlLastValue){
		//Todo: thuc hien truy van
		if i.PagingEnabled {
			//Todo: Error
		}else {
			//Todo: lấy hết dữ liệu trong 1 câu truy vấn
		}
	}
	//
	if i.UserColumn && !isSqlLastValue {
		return errors.New("")
	}
	//
	if !i.UserColumn && i.RecordLastRun {
		return errors.New("")
	}


	if i.RecordLastRun {
		//TODO: cột được chỉ định bởi tracking_column để theo dõi giá trị cuối cùng. Giá trị cuối cùng này được lưu trữ trong metadata và cập nhật sau mỗi lần chạy.

	}else {
		//TODO: Cột được chỉ định bởi tracking_column để theo dõi giá trị cuối cùng, nhưng không lưu trữ giá trị cuối cùng trong metadata. Mỗi lần chạy, Logstash sẽ truy vấn dữ liệu từ giá trị cuối cùng đã lưu trữ trước đó và cập nhật giá trị cuối cùng trong bộ nhớ tạm

	}

	return nil
}

func (i *InputConfig) CreateStatement(statement string) (string, error){
	//sqlLastValue := 123123123123
	isSqlValue := strings.Contains(statement, ":sql_last_value")

	if i.CleanRun {
		if !isSqlValue {
			return statement,nil
		}else {
			switch i.TrackingColumnType {
			case "numberic":
				statement = strings.ReplaceAll(statement,":sql_last_value","0")
				return statement,nil
			case "string":
				statement = strings.ReplaceAll(statement,":sql_last_value","")
				return statement,nil
			case "timestamps":
				statement = strings.ReplaceAll(statement,"sql_last_value", "1970-01-01T00:00:00Z")
				return statement,nil
			default:
				return statement, errors.New("tracking_column_type invalid")
			}
		}
	}else {
		if i.RecordLastRun || i.UserColumn {

		}
	}

	return "", nil
}

func (i *InputConfig) FetchResults(statement string)  {
	//tableName := getMainTableFromQuery(statement)
	//count = i.Client.Raw("Select count(*) from ")

	//recordCount := 0
	//pageCount := 0
	//for {
	//	var results []map[string]interface{}
	//
	//}
}

func (i *InputConfig) PushEvent(results []map[string]interface{}) error  {
	return nil
}

func (i *InputConfig) updateLastSyncedValued() error {
	return nil
}

func (i *InputConfig) Start(ctx context.Context, msg chan<- event.Event) error {
	//var err error

	select {
	case <-ctx.Done():
		logrus.Error("CyM_Agent input redis stopped")
		return nil
	default:
	}

	return nil
}



func findMainTable(statement string) (string,error) {
	stmt, err := sqlparser.Parse(statement)
	if err != nil {
		return "", errors.New(fmt.Sprint("Error parsing SQL:", err))
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		tableExprs := stmt.From
		if len(tableExprs) > 0 {
			switch tableExpr := tableExprs[0].(type) {
			case *sqlparser.AliasedTableExpr:
				tableName := sqlparser.String(tableExpr.Expr)
				return tableName,nil
			default:
				return "", errors.New("unexpected table expression type")
			}
		} else {
			return "", errors.New("no tables found in the SQL")
		}
	default:
		return "", errors.New("unexpected statement type")
	}
}

