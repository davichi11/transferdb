package tests

import (
	"database/sql"
	"github.com/wentaojin/transferdb/server"
	"github.com/wentaojin/transferdb/service"
	"testing"
)

func TestConnection(t *testing.T) {
	oraCfg := service.SourceConfig{
		Username:      "PWKSH",
		Password:      "Zhksh_2020",
		Host:          "172.16.100.7",
		Port:          11521,
		ServiceName:   "PWKSHPDB",
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=10&timezone=Local",
		SessionParams: []string{"alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'"},
		SchemaName:    "PWKSHPDB",
		IncludeTable:  nil,
		ExcludeTable:  nil,
	}
	sqlDB, err := server.NewOracleDBEngine(oraCfg)
	if err != nil {
		t.Error(err)
		return
	}

	engine := service.Engine{
		OracleDB: sqlDB,
	}
	defer func(OracleDB *sql.DB) {
		err := OracleDB.Close()
		if err != nil {
			t.Error("数据库关闭异常", err)
		}
	}(engine.OracleDB)

	cols, res, err := service.Query(engine.OracleDB, "select oid, yxbh, sbmc from T_TX_ZNYC_DZ where ROWNUM <=10")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(cols)
	t.Log(res)
}
