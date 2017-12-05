package base

import "net/http"

//相应对象
type Response struct {
	httpresp *http.Response
	depth uint32
}
//创建新的相应对象
func NewResponse(httpresp *http.Response,depth uint32) *Response {

	return &Response{httpresp,depth}
}
//获取相应对象
func (resp *Response) Httpresp()  *http.Response {
	return resp.httpresp
}
//获取相应深度
func (resp *Response) Depth()  uint32 {
	return resp.depth
}
//相应数据是否有效
func (resp *Response) Valid()bool  {

	return resp.httpresp!=nil&&resp.httpresp.Body!=nil
}