package itempipeline

import (
	"spider/crawler/base"
	"errors"
	"fmt"
	"sync/atomic"
)

//条目处理管道的接口类型
type ItemPipeline interface {
	//发送条目
	Send(item base.Item)[]error
	//FailFast方法会返回一个布尔值，该值表示当前的条目处理管道是否为快速失败的
	//这里的快速失败是指：只要对某个条目的处理流程在某个步骤上出错
	//name处理条目就会忽略调后面的所有处理步骤并报告错误
	FailFast()bool
	//设置是否快速失败
	SetFailFast(bool bool)
	//获得已发送、已接受和已处理的条目的计数值
	//更确切地说，作为结果值的切片总会有三个元素值。这三个值分别代表前述的三个计数
	Count() []uint64
	//获取正在处理条目的数量
	ProcrssingNumber()int64
	//获取摘要信息
	Sumary()string
	//
	//
}
//被用来处理条目的函数类型
type ProcessItem func(item base.Item)(result base.Item,err error)
//条目处理管道的实现类型
type myItemPipeline struct {
	itemProcessors []ProcessItem//条目处理器列表
	failFast bool//表示处理是否需要快速失败的标志位
	sent uint64//表示已经发送条目的数量
	accepted uint64//已经接受的条目的数量
	processed uint64//已被处理的条目的数量
	processingNumber uint64//正在处理条目的数量
}

func NewItemPipeline(items []ProcessItem)ItemPipeline  {
	if items==nil {
		panic(errors.New(fmt.Sprintf("Invalid item processor list")))
	}
	innerItemProcessItem:=make([]ProcessItem,0)
	for i,ip:=range items {
		if ip==nil {
			panic(errors.New(fmt.Sprintf("Invalid item processor [%d]\n",i)))
		}
		innerItemProcessItem=append(innerItemProcessItem, ip)
	}
	itemline:=myItemPipeline{innerItemProcessItem,false,uint64(0),uint64(0),uint64(0),uint64(0)}
	return &itemline
}
/**
1.检查ip的有效性
2.依次调用itemProcessors中的条目字段对有效的条目进行处理
3.收集处理过程中发生的错误
4.在整个处理过程中实时的对
sent uint64//表示已经发送条目的数量
accepted uint64//已经接受的条目的数量
processed uint64//已被处理的条目的数量
processiongNumber uint64//正在处理条目的数量
字段进行处理
 */
//发送条目
func (ip *myItemPipeline)Send(item base.Item)[]error{
	atomic.AddUint64(&ip.processingNumber,1)//正在处理条目的数量---加
	defer atomic.AddUint64(&ip.processingNumber,^uint64(0))//正在处理条目的数量---减
	atomic.AddUint64(&ip.sent,1)//---已经发送的条目加
	errs:=make([]error,0)
	if item==nil {
		return append(errs, errors.New(fmt.Sprintf("Ihe item is Invalid")))
	}
	atomic.AddUint64(&ip.accepted,1)//已经接受的条目的数量
	var currentItem base.Item=item
	for _,itemProcessor:=range ip.itemProcessors  {
		processorItem,err:=itemProcessor(currentItem)
		if err!=nil {
			errs=append(errs, err)
			if ip.failFast {
				break
			}
		}
		if processorItem!=nil {
			currentItem=processorItem
		}
	}
	atomic.AddUint64(&ip.processed,1)//已被条目处理器处理的条目的数量
	return errs
}
//FailFast方法会返回一个布尔值，该值表示当前的条目处理管道是否为快速失败的
//这里的快速失败是指：只要对某个条目的处理流程在某个步骤上出错
//name处理条目就会忽略调后面的所有处理步骤并报告错误
func (ip *myItemPipeline)FailFast()bool{
	return ip.failFast
}
//设置是否快速失败
func (ip *myItemPipeline)SetFailFast(bool bool){
	ip.failFast=bool
}
//获得已发送、已接受和已处理的条目的计数值
//更确切地说，作为结果值的切片总会有三个元素值。这三个值分别代表前述的三个计数
func (ip *myItemPipeline)Count() []uint64{
	counts:=make([]uint64,3)
	counts[0]=atomic.LoadUint64(&ip.sent)//发送
	counts[1]=atomic.LoadUint64(&ip.accepted)//接收
	counts[2]=atomic.LoadUint64(&ip.processed)//处理
	return counts
}
//获取正在处理条目的数量
func (ip *myItemPipeline)ProcrssingNumber()int64{
	return int64(atomic.LoadUint64(&ip.processingNumber))
}
var summaryTemplate="failFast: %v,processorNumber: %d,sent: %d,accepted: %d,processed: %d,processiongNumber: %d,"
//获取摘要信息
func (ip *myItemPipeline)Sumary()string{
	counts:=ip.Count()
	summary:=fmt.Sprintf(summaryTemplate,ip.FailFast(),len(ip.itemProcessors),counts[0],counts[1],counts[2],ip.ProcrssingNumber())
	return summary
}