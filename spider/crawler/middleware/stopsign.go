package middleware

import (
	"sync"
	"fmt"
)

//停止信号的皆苦类型
type StopeSign interface {
	//置位挺信号，相当于发出停止信号
	//如果先前已经发出停止信号，那么该方法就会返回false
	Sign()bool
	//判断停止信号是否已经发出
	Signed()bool
	//重置停止信号。相当于收回停止信号，并清除所有停止信号处理记录
	Reset()
	//处理停止信号
	//参数code应该为停止信号的代号该代号会出现在停止信号的记录中
	Deal(code string)
	//处理停止信号
	//参数code因该停止信号处理方式的代号。该代号会出现在停止信号的处理记录中
	DealCount(code string)uint32
	//获取停止信号被处理的总数
	DealTotal()uint32
	//获取摘要信息。其中因该包含所有的停止信号处理记录
	Summary()string
	//
	//
}
type myStopeSign struct {
	dealCountMap map[string]uint32//处理计数器的字典
	signed bool//判断信号
	mutex sync.Mutex
}

func NewStopeSign()StopeSign  {
	ss:=&myStopeSign{make(map[string]uint32),false,*new(sync.Mutex)}
	return ss
}
//置位挺信号，相当于发出停止信号
//如果先前已经发出停止信号，那么该方法就会返回false
func (myss *myStopeSign) Sign()bool{
	myss.mutex.Lock()
	defer myss.mutex.Unlock()
	if myss.signed {
		return false
	}
	myss.signed=true
	return true
}

//判断停止信号是否已经发出
func (myss *myStopeSign) Signed()bool{
	return myss.signed
}
//重置停止信号。相当于收回停止信号，并清除所有停止信号处理记录
func (myss *myStopeSign) Reset(){
	myss.mutex.Lock()
	defer myss.mutex.Unlock()
	myss.signed=false
	myss.dealCountMap=make(map[string]uint32)
}
//处理停止信号
//参数code为该停止信号处理方式的代号。该代号会出现在停止信号的处理记录中
func (myss *myStopeSign)DealCount(code string)uint32{
	myss.mutex.Lock()
	defer myss.mutex.Unlock()
	v,ok:=myss.dealCountMap[code]
	if !ok {
		return uint32(0)
	}else {
		return v
	}
}
//获取停止信号被处理的总数
func (myss *myStopeSign)DealTotal()uint32{
	myss.mutex.Lock()
	defer myss.mutex.Unlock()
	dealCountMap:=myss.dealCountMap
	dealCount:=uint32(0)
	for _,v:=range dealCountMap  {
		dealCount+=v
	}
	return dealCount
}
var myStopeSignTemplate="dealCountMap:%d,signed:%d"
//获取摘要信息。其中因该包含所有的停止信号处理记录
func (myss *myStopeSign)Summary()string{
	sumary:=fmt.Sprintf(myStopeSignTemplate,len(myss.dealCountMap),myss.signed )
	return sumary
}

//处理
func (myss *myStopeSign) Deal(code string){
	myss.mutex.Lock()
	defer myss.mutex.Unlock()
	if !myss.signed {
		return
	}
	if _,ok:=myss.dealCountMap[code];!ok {
		myss.dealCountMap[code]=1
	}else {
		myss.dealCountMap[code]+=1
	}
}