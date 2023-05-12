package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
	"jbdc/event"
	"jbdc/metadata"
	"math"
	"strconv"
	"strings"
	"time"
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

	Metadata metadata.Metadata
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

func (i *InputConfig) Execute(ca *cache.Cache) error {
	isSqlLastValue := strings.Contains(i.Statement,":sql_last_value")
	//
	if (i.CleanRun && isSqlLastValue) || (!i.CleanRun && !i.RecordLastRun && !i.UserColumn && isSqlLastValue) {

		//Todo: thuc hien truy van
		if i.PagingEnabled {
			//Todo: dùng tracking_value và :sql_value để thực hiện theo dõi theo đồng bộ với fetch_size
			i.FetchResults(ca)
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

func (i *InputConfig) FetchResults(ca *cache.Cache) error {
	fetchCount := int64(0)
	page := int(math.Ceil(float64(i.FetchSize)/float64(i.PageSize)))
	var results []map[string]interface{}

	for {
		pageCount := int64(0)

		var sqlLastValue interface{}
		if !i.RecordLastRun {
			sqlLastValue, ok := ca.Get(i.Statement)
			if !ok {
				var statement string
				switch i.TrackingColumnType {
				case "numberic":
					statement = strings.ReplaceAll(i.Statement,":sql_last_value",fmt.Sprintf("%v",0))

				case "timestamp":
					statement = strings.ReplaceAll(i.Statement,":sql_last_value",fmt.Sprintf("%s","1970-01-01T00:00:00Z"))

				case "string":
					statement = strings.ReplaceAll(i.Statement,":sql_last_value",fmt.Sprintf("%s",""))
				default:
					return errors.New(fmt.Sprintf("Unsupported tracking column type: %s", i.TrackingColumnType))
				}


			//	truy vấn
			}else {
				statement := strings.ReplaceAll(i.Statement,":sql_last_value",fmt.Sprintf("%s",sqlLastValue))
				dns := fmt.Sprintf("%s LIMIT %s ", statement, fmt.Sprintf("%v",i.FetchSize) )
				rows , _ :=  i.Client.Raw(dns).Rows()
				// ... .. . .. . => results
				if int64(len(results)) < i.FetchSize {

				}else {
					for i := 0; i < page; i++ {

					}
				}
			}


		} else {
			sqlLastValue =
		}

	}

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

