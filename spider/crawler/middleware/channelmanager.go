package middleware

import (
	"spider/crawler/base"
	"errors"
	"sync"
	"fmt"
	"github.com/Sirupsen/logrus"
)
var logger *logrus.Logger= base.NewLogger()
//通道管理器的接口类型
type ChannelManager interface {
	//初始化通道管理器
	//参数channelLen代表通道管理器中的各类通道的初始长度
	//参数reset指明是否重新初始化通道管理器
	Init(channelArgs base.ChannelArgs,reset bool)bool
	//关闭通道管理器
	Close()bool
	//获取请求传输通道
	ReqChan()(chan base.Request,error)
	//获取相应传输通道
	RespChan()(chan base.Response,error)
	//获取条目传输通道
	ItemChan()(chan base.Item,error)
	//获取错误传输通道
	ErrorChan()(chan error,error)
	//获取通道长度值
	ChannelLen()  base.ChannelArgs
	//获取通道管理器的状态
	Status()ChannelManagerStatus
	//获取摘要信息
	Summary()string
	//
}
//用来表示通道管理状态的类型
type  ChannelManagerStatus uint8
////默认通道长度
//var defaultChanlen=uint(10)
const   (
	CHANNEL_MANAGER_STATUS_UNINITIALIZED ChannelManagerStatus=0//未初始化状态
	CHANNEL_MANAGER_STATUS_INITIALIZED ChannelManagerStatus=1//已初始化状态
	CHANNEL_MANAGER_STATUS_CLOSED  ChannelManagerStatus=2//已关闭状态
)
//通道管理类的实现基类
type myChannelManager struct {
	channelArgs base.ChannelArgs	//通道参数容器
	reqCh chan base.Request//请求通道
	respCh chan base.Response//响应通道
	itemCh chan base.Item//条目通道
	errCh chan error//错误通道
	status ChannelManagerStatus//通道管理器的状态
	rwnutex sync.RWMutex//读写锁
}
//创建通道管理器
//如果参数channelLen的值为零就会使用默认值代替
func NewChannelManager(channelArgs base.ChannelArgs)ChannelManager  {
	chanman:=&myChannelManager{}
	chanman.Init(channelArgs,true)
	return chanman
}
func (chanman *myChannelManager)Init(channelArgs base.ChannelArgs,reset bool) bool {
	if err:=channelArgs.Check();err!=nil{
		panic(err)
	}
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if chanman.status==CHANNEL_MANAGER_STATUS_INITIALIZED &&!reset{//当已经初始化并且不重写就
		return false
	}
	chanman.channelArgs=channelArgs
	chanman.reqCh=make(chan base.Request,channelArgs.ReqChanLen())
	chanman.respCh=make(chan base.Response,channelArgs.RespChanLen())
	chanman.itemCh=make(chan base.Item,channelArgs.ItemChanLen())
	chanman.errCh=make(chan error,channelArgs.ErrorChanLen())
	chanman.status=CHANNEL_MANAGER_STATUS_INITIALIZED
	return true
}
//关闭通道
func (chanman *myChannelManager)Close() bool {
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if chanman.status!=CHANNEL_MANAGER_STATUS_INITIALIZED {
		return false
	}
	close(chanman.reqCh)
	close(chanman.respCh)
	close(chanman.itemCh)
	close(chanman.errCh)
	chanman.status=CHANNEL_MANAGER_STATUS_CLOSED
	return true
}
//检查状态。在获取通道的时候，通道管理器应处于已初始化状态
//如果通道管理器处于初始化状态就会返回一个非nil的错误值
func (chanman *myChannelManager)chaeckStatus() error {
	if chanman.status==CHANNEL_MANAGER_STATUS_INITIALIZED{
		return nil
	}
	statusName,ok:=statusNameMap[chanman.status]
	if !ok {
		statusName=fmt.Sprintf("%d",chanman.status)
	}
	errMsg:=fmt.Sprintf("The undesirable status of channal manager:%s!\n",statusName)
	return errors.New(errMsg)
}
//表示状态代码与状态名称之间的映射关系的字典
var statusNameMap =map[ChannelManagerStatus]string{
CHANNEL_MANAGER_STATUS_UNINITIALIZED :"uninitialized",//未初始化状态
CHANNEL_MANAGER_STATUS_INITIALIZED :"initialized",//已初始化状态
CHANNEL_MANAGER_STATUS_CLOSED :"closed",//已关闭状态
}

//获取请求传输通道
func(chanman *myChannelManager) ReqChan()(chan base.Request,error){
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if err:=chanman.chaeckStatus();err!=nil {
	return nil,err
	}
	return chanman.reqCh,nil
}
//获取相应传输通道
func(chanman *myChannelManager) RespChan()(chan base.Response,error){
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if err:=chanman.chaeckStatus();err!=nil {
		return nil,err
	}
	return chanman.respCh,nil
}
//获取条目传输通道
func(chanman *myChannelManager) ItemChan()(chan base.Item,error){
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if err:=chanman.chaeckStatus();err!=nil {
		return nil,err
	}
	return chanman.itemCh,nil
}
//获取错误传输通道
func(chanman *myChannelManager)ErrorChan()(chan error,error){
	chanman.rwnutex.Lock()
	defer chanman.rwnutex.Unlock()
	if err:=chanman.chaeckStatus();err!=nil {
		return nil,err
	}
	return chanman.errCh,nil
}
//获取通道长度值
func(chanman *myChannelManager)ChannelLen() base.ChannelArgs{
	return chanman.channelArgs
}
//获取通道管理器的状态
func(chanman *myChannelManager)Status()ChannelManagerStatus{
	return chanman.status
}
//摘要的模板
var chanmanSummaryTemplate="status:%s, requestChannel:%d/%d, responseChannel:%d/%d, itemChannel: %d/%d, errChannel:%d/%d"
//获取摘要信息
func(chanman *myChannelManager)Summary()string{
sumary:=fmt.Sprintf(chanmanSummaryTemplate,
	statusNameMap[chanman.status], len(chanman.reqCh),cap(chanman.reqCh),
	len(chanman.respCh),cap(chanman.respCh),
	len(chanman.itemCh),cap(chanman.itemCh),
	len(chanman.errCh),cap(chanman.errCh))
	return sumary
}
