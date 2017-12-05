package scheduler

import (
	"spider/crawler/base"
	"sync"
	"fmt"
)

//请求缓存的接口类型
type requestCache interface {
	//将请求放入缓存中
	put(req *base.Request)bool
	//从请求中缓存获取最早被放入缓存中的请求
	get()*base.Request
	//获取请求缓存的容量
	capacity()int
	//获取请求缓存的实际长度，即请求的即时长度
	length()int
	//关闭请求缓存
	close()
	//获取请求缓存的摘要信息
	summary() string
}
//创建请求缓存
func newRequestCache() requestCache{
	rc:=&myRequestCache{
		cache:make([]*base.Request,0),
	}
	return rc
}

type myRequestCache struct {
	cache []*base.Request//储存所有请求
	status byte//状态0为正在运行1为关闭
	mutex sync.Mutex//确保并发安全
}

//将请求放入缓存中
func (rcache *myRequestCache)put(req *base.Request)bool{

	if req==nil {
		return false
	}
	if rcache.status==1 {
		return false
	}
	rcache.mutex.Lock()
	defer rcache.mutex.Unlock()
	rcache.cache=append(rcache.cache, req)
	return true
}
//从请求中缓存获取最早被放入缓存中的请求
func (rcache *myRequestCache)get()*base.Request{
	if rcache.length()==0 {
		return nil
	}
	if rcache.status==1 {
		return nil
	}
	rcache.mutex.Lock()
	defer rcache.mutex.Unlock()
	req:=rcache.cache[0]
	rcache.cache=rcache.cache[1:]
	return req
}
//获取请求缓存的容量
func (rcache *myRequestCache)capacity()int{
	return cap(rcache.cache)
}
//获取请求缓存的实际长度，即请求的即时长度
func (rcache *myRequestCache)length()int{
	return len(rcache.cache)
}
//关闭请求缓存
func (rcache *myRequestCache)close(){
	if rcache.status==1 {
		return
	}
	rcache.status=1
}
//摘要的信息模板
var sumaryTemplate="status: %s, length: %d, capacity：%d \n"
var statusMap=map[byte]string{
	0:"running",
	1:"closed",
}
//获取请求缓存的摘要信息
func (rcache *myRequestCache)summary() string{
	summary:=fmt.Sprintf(sumaryTemplate,statusMap[rcache.status],rcache.length(),rcache.capacity())
	return summary
}