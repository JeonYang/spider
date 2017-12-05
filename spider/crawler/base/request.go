package base

import "net/http"
//请求
type Request struct {
	httpReq *http.Request//HTTP请求指针值
	depth uint32//请求深度
}
//创建新的请求
func NewRequest(httpReq *http.Request,depth uint32) *Request  {

	return & Request{httpReq,depth}
}
//获取请求值
func (req *Request) HttpReq() *http.Request {
	return req.httpReq
}
//获取请求深度
func (req *Request) Depth() uint32 {
	return req.depth
}
//请求数据是否有效
func (req *Request)  Valid()bool  {

	return req.httpReq!=nil&&req.httpReq.Body!=nil
}