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
package taskflow

import (
	"encoding/json"
	"sync"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

type IncrPayload struct {
	GlobalSCN      int             `json:"global_scn"`
	SourceTableSCN int             `json:"source_table_scn"`
	SourceSchema   string          `json:"source_schema"`
	SourceTable    string          `json:"source_table"`
	TargetSchema   string          `json:"target_schema"`
	TargetTable    string          `json:"target_table"`
	Operation      string          `json:"operation"`
	OracleRedo     string          `json:"oracle_redo"` // Oracle 已执行 SQL
	MySQLRedo      []string        `json:"mysql_redo"`  // MySQL 待执行 SQL
	OperationType  string          `json:"operation_type"`
	Engine         *service.Engine `json:"-"`
}

type IncrResult struct {
	Payload IncrPayload
	Status  bool
}

// 任务同步
func (p *IncrPayload) Run() error {
	// 数据写入并更新元数据表
	if err := applierTableIncrementRecord(p); err != nil {
		service.Logger.Error("apply table increment record failed",
			zap.String("payload", p.Marshal()),
			zap.Error(err))
		return err
	}
	return nil
}

// 序列化
func (p *IncrPayload) Marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		service.Logger.Error("marshal task to string",
			zap.String("string", string(b)),
			zap.Error(err))
	}
	return string(b)
}

func CreateWorkerPool(numOfWorkers int, jobQueue chan IncrPayload, resultQueue chan IncrResult) {
	var wg sync.WaitGroup
	for i := 0; i < numOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, jobQueue, resultQueue)
	}
	wg.Wait()
	close(resultQueue)
}

func GetIncrResult(done chan bool, resultQueue chan IncrResult) {
	for result := range resultQueue {
		if !result.Status {
			service.Logger.Fatal("task increment table record",
				zap.String("payload", result.Payload.Marshal()))
		}
	}
	done <- true
}

func worker(wg *sync.WaitGroup, jobQueue chan IncrPayload, resultQueue chan IncrResult) {
	defer wg.Done()
	for job := range jobQueue {
		if err := job.Run(); err != nil {
			result := IncrResult{
				Payload: job,
				Status:  false,
			}
			resultQueue <- result
		}
		result := IncrResult{
			Payload: job,
			Status:  true,
		}
		resultQueue <- result
	}
}