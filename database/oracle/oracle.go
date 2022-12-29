/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"strings"
)

type Oracle struct {
	Ctx      context.Context
	OracleDB *sql.DB
}

// 创建 oracle 数据库引擎
func NewOracleDBEngine(ctx context.Context, oraCfg config.OracleConfig) (*Oracle, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// https://godror.github.io/godror/doc/connection.html
	// You can specify connection timeout seconds with "?connect_timeout=15" - Ping uses this timeout, NOT the Deadline in Context!
	// For more connection options, see [Godor Connection Handling](https://godror.github.io/godror/doc/connection.html).

	connString := fmt.Sprintf("oracle://%s:%s@%s:%d/%s", oraCfg.Username, oraCfg.Password, oraCfg.Host, oraCfg.Port, oraCfg.ServiceName)
	// godror logger 日志输出
	// godror.SetLogger(zapr.NewLogger(zap.L()))

	//sqlDB := sql.OpenDB(godror.NewConnector(oraDSN))
	sqlDB, err := sql.Open("oracle", connString)
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetConnMaxLifetime(0)

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return &Oracle{
		Ctx:      ctx,
		OracleDB: sqlDB,
	}, nil
}

func Query(ctx context.Context, db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.QueryContext(ctx, querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("general sql [%v] query rows.Scan failed: [%v]", querySQL, err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			// 查询字段值 NULL
			// 如果字段值 = NULLABLE 则表示值是 NULL
			// 如果字段值 = "" 则表示值是空字符串
			// 如果字段值 = 'NULL' 则表示值是 NULL 字符串
			// 如果字段值 = 'null' 则表示值是 null 字符串
			if v == nil {
				row[cols[k]] = "NULLABLE"
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[cols[k]] = string(v)
			}
		}
		res = append(res, row)
	}

	if err = rows.Err(); err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())
	}
	return cols, res, nil
}

func (o *Oracle) GetOracleSchemas() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	cols, res, err := Query(o.Ctx, o.OracleDB, `SELECT DISTINCT username FROM DBA_USERS`)
	if err != nil {
		return schemas, err
	}
	for _, col := range cols {
		for _, r := range res {
			schemas = append(schemas, common.StringUPPER(r[col]))
		}
	}
	return schemas, nil
}

func (o *Oracle) GetOracleSchemaTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT table_name AS TABLE_NAME FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, r := range res {
		tables = append(tables, strings.ToUpper(r["TABLE_NAME"]))
	}

	return tables, nil
}
