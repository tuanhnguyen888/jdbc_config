package postgres

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSQL(t *testing.T) {
	dns := "host=localhost port=5432 user=postgres dbname=user sslmode=disable password=khong123"
	db, err :=gorm.Open("postgres",dns)
	assert.NoError(t, err)
	var number interface{}
	err = db.Raw("SELECT * FROM logs").Count(&number).Error
	logrus.Error(err)
	logrus.Info(number)
}


func TestName(t *testing.T) {
	sql := "select (SELECT UTL_INADDR.get_host_address from dual) as db_ip,user_name,os_user,host,ip_address,service_name,module,server_host,authentication_method,authenticated_identity,to_char(create_time, 'yyyymmddhh24miss') as cus_time from SYSTEM.log_login where  create_time>=trunc(sysdate)-1 and create_time<trunc(sysdate) and ip_address is not null AND to_char(create_time, 'yyyymmddhh24miss') > :sql_last_value ORDER BY create_time ASC"
	nameTable, err := findMainTable(sql)
	assert.NoError(t, err)
	logrus.Info(nameTable)

	sql = "SELECT event_id,start_time,end_time,username,client_ip,action,object_type,result,object_code FROM Gateprov3.sys_event_log where ACTION not in ('ACTION_LOG_OPEN_FW','ACTION_LOG_CLOSE_FW') AND event_id > :sql_last_value AND event_id > 19416000 ORDER BY event_id ASC"
	nameTable,err = findMainTable(sql)
	assert.NoError(t, err)
	logrus.Info(nameTable)

	sql = "SELECT * FROM main_table JOIN sub_table ON main_table.id = sub_table.main_id"
	nameTable,err = findMainTable(sql)
	assert.NoError(t, err)
	logrus.Info(nameTable)
}