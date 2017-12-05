package tool

import (
	"spider/crawler/scheduler"
	"time"
	"errors"
	"fmt"
	"runtime"
)
// 摘要信息的模板。
var summaryForMonitoring = "Monitor - Collected information[%d]:\n" +
	"  Goroutine number: %d\n" +
	"  Scheduler:\n%s" +
	"  Escaped time: %s\n"
// 已达到最大空闲计数的消息模板。
var msgReachMaxIdleCount = "The scheduler has been idle for a period of time" +
	" (about %s)." +
	" Now consider what stop it."
// 停止调度器的消息模板。
var msgStopScheduler = "Stop scheduler...%s."
//日志记录函数的类型
//参数level代表日志级别，级别设定：0：普通，1：警告；2：错误
type Record func(lovel byte,cotent string)
func Monitoring(scheduler scheduler.Scheduler,intervalNs time.Duration,maxIdleCount uint,autoStop bool,detailSummary bool,record Record)<-chan uint64 {
	if scheduler==nil{
		panic(errors.New("The scheduler is invalid!"))
	}
	//防止过小的参数值对爬取流程的影响
	if intervalNs<time.Millisecond {
		intervalNs=time.Millisecond
	}
	if maxIdleCount<1000 {
		maxIdleCount=1000
	}
	//监控停止通知器
	stopNotifier:=make(chan byte,1)
	//接收和报告错误
	reportError(scheduler,record,stopNotifier)
	//记录摘要信息
	recordSummary(scheduler,detailSummary,record,stopNotifier)
	//检查计数器通道
	checkCountChan:=make(chan uint64,2)
	//检查空闲状态
	checkStatus(scheduler,intervalNs,maxIdleCount,autoStop,checkCountChan,record,stopNotifier)
	return checkCountChan
}
//接收和报告错误
func reportError(scheduler scheduler.Scheduler,record Record,stopNotifier <-chan byte){
	go func() {
		//等待调度器开启
		waitForSchedulerStart(scheduler)
		for   {
			//查看监控停止通知器
			select {
			case <-stopNotifier:
				return
			default:

			}
			errorChan:=scheduler.ErrorChan()
			if errorChan==nil{
				return
			}
			err:=<-errorChan
			if err!=nil {
				errMsg:=fmt.Sprintf("Error (received from error channel):%s",err)
				record(2,errMsg)
			}
			time.Sleep(time.Millisecond)
		}
	}()
}
//等待调度器开启
func waitForSchedulerStart(scheduler scheduler.Scheduler){
	for !scheduler.Runing()  {
		time.Sleep(time.Millisecond)
	}
}
//记录摘要信息
func recordSummary(schedul scheduler.Scheduler,detailSummary bool,record Record,stopNotifier <-chan byte){

	go func() {
		// 等待调度器开启
		waitForSchedulerStart(schedul)
		// 准备
		var recordCount uint64=1
		startTime:=time.Now()
		var prevSchedSummary scheduler.SchedSummary
		var prevNumGoroutine int
		for   {
			select {
			case <-stopNotifier:
				return
			default:
			}
			currNumGoroutine:=runtime.NumGoroutine()
			currSchedSummary:=schedul.Summary(" ")
			if currNumGoroutine != prevNumGoroutine ||
				!currSchedSummary.Same(prevSchedSummary) {
				schedSummaryStr := func() string {
					if detailSummary {
						return currSchedSummary.Detail()
					} else {
						return currSchedSummary.String()
					}
				}()
				// 记录摘要信息
				info := fmt.Sprintf(summaryForMonitoring,
					recordCount,
					currNumGoroutine,
					schedSummaryStr,
					time.Since(startTime).String(),
				)
				record(0, info)
				prevNumGoroutine = currNumGoroutine
				prevSchedSummary = currSchedSummary
				recordCount++
			}
			time.Sleep(time.Microsecond)
		}
	}()
}
//检查空闲状态
// 检查状态，并在满足持续空闲时间的条件时采取必要措施。
func checkStatus(
	scheduler scheduler.Scheduler,
	intervalNs time.Duration,
	maxIdleCount uint,
	autoStop bool,
	checkCountChan chan<- uint64,
	record Record,
	stopNotifier chan<- byte) {
	var checkCount uint64
	go func() {
		defer func() {
			stopNotifier <- 1
			stopNotifier <- 2
			checkCountChan <- checkCount
		}()
		// 等待调度器开启
		waitForSchedulerStart(scheduler)
		// 准备
		var idleCount uint
		var firstIdleTime time.Time
		for {
			// 检查调度器的空闲状态
			if scheduler.Idle() {
				idleCount++
				if idleCount == 1 {
					firstIdleTime = time.Now()
				}
				if idleCount >= maxIdleCount {
					msg :=
						fmt.Sprintf(msgReachMaxIdleCount, time.Since(firstIdleTime).String())
					record(0, msg)
					// 再次检查调度器的空闲状态，确保它已经可以被停止
					if scheduler.Idle() {
						if autoStop {
							var result string
							if scheduler.Stop() {
								result = "success"
							} else {
								result = "failing"
							}
							msg = fmt.Sprintf(msgStopScheduler, result)
							record(0, msg)
						}
						break
					} else {
						if idleCount > 0 {
							idleCount = 0
						}
					}
				}
			} else {
				if idleCount > 0 {
					idleCount = 0
				}
			}
			checkCount++
			time.Sleep(intervalNs)
		}
	}()
}