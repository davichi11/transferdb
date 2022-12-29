package tests

import (
	"context"
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/oracle"
	"testing"
)

func TestConnection(t *testing.T) {
	oraCfg := config.OracleConfig{
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
	oracleDb, err := oracle.NewOracleDBEngine(context.Background(), oraCfg)
	if err != nil {
		t.Error(err)
		return
	}

	cols, res, err := oracle.Query(context.Background(), oracleDb.OracleDB, "select oid, yxbh, sbmc from T_TX_ZNYC_DZ where ROWNUM <=10")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(cols)
	t.Log(res)
	for k, v := range res[0] {
		t.Log(k + "," + v)
	}
}

type Dz struct {
	ObjId string `db:"name:OBJ_ID"`
	Ssds  string `db:"name:SSDS"`
	Name  string `db:"name:BDZMC"`
}

func TestQueryData(t *testing.T) {
	oraCfg := config.OracleConfig{
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
	connString := fmt.Sprintf("oracle://%s:%s@%s:%d/%s", oraCfg.Username, oraCfg.Password, oraCfg.Host, oraCfg.Port, oraCfg.ServiceName)
	conn, err := go_ora.NewConnection(connString)
	if err != nil {
		fmt.Println("Can't create connection: ", err)
		return
	}
	err = conn.Open()
	if err != nil {
		fmt.Println("Can't open connection: ", err)
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println("Can't close connection: ", err)
		}
	}()

	//err = conn.RegisterType("SDO_GEOMETRY", Geometry{})
	//if err != nil {
	//	t.Error("SDO_GEOMETRY 类型注册异常", err)
	//	return
	//}
	// check for err

	stmt := go_ora.NewStmt("select * from T_SB_ZNYC_DZ where ROWNUM <=10", conn)
	rows, err := stmt.Query_(nil)
	if err != nil {
		t.Error("查询异常", err)
		return
	}
	for rows.Next_() {
		var dz Dz
		err := rows.Scan(&dz)
		if err != nil {
			t.Error("数据绑定异常", err)
			continue
		}
		t.Log("查询结果,OBJ_ID=", dz.ObjId, "SSDS=", dz.Ssds, "BDZMC=", dz.Name)
	}
}

type Geometry struct {
	SdoGtype     int64     `oracle:"name:SDO_GTYPE"`
	SdoSrid      int64     `oracle:"name:SDO_SRID"`
	SdoPoint     Point     `oracle:"name:SDO_POINT"`
	SdoElemInfo  []int64   `oracle:"name:SDO_ELEM_INFO"`
	SdoOrdinates []float64 `oracle:"name:SDO_ORDINATES"`
}

type Point struct {
	X float64 `oracle:"name:X"`
	Y float64 `oracle:"name:Y"`
	Z float64 `oracle:"name:Z"`
}
