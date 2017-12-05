package analyzer

import (
	"spider/crawler/base"
	"net/http"
	"errors"
	"net/url"
	"fmt"
	"github.com/Sirupsen/logrus"
	"spider/crawler/middleware"
	"reflect"
)
var logger *logrus.Logger=base.NewLogger()
//分析器的接口类型
type Analyzer interface {
	Id() uint32//获取ID
	Analuze(respParsers []ParseResponse,resp base.Response)([]base.Data,[]error)//根据规则分析响应并返回请求和条目
}
//被用于解析HTTP相应的函数类型
type ParseResponse func(httpresp *http.Response,respDepth uint32) ([]base.Data,[]error)
//分析器池的接口类型
type AnalyzerPool interface {
	Take()(Analyzer,error)//从池中取出一个
	Return(analyzer Analyzer)error//把一个归还给分析器
	Total()uint32//获得分析器的总容量
	Used()uint32//获得正在使用的分析器数量
}
//分析器的实现类型
type myAnalyzer struct {
	id uint32//Id
}
//Id生成器
var analyzerIdGenertor middleware.IdGenertor = middleware.NewIdGenertor()
//创建分析器
func NewAnalyzer()Analyzer  {
	return &myAnalyzer{genAnalyzerId()}
}
func genAnalyzerId() uint32 {
	return analyzerIdGenertor.GetUint32()
}
//获取ID
func (analyzer *myAnalyzer)Id() uint32{
	return analyzer.id
}
//根据规则分析响应并返回请求和条目
func (analyzer *myAnalyzer)Analuze(respParsers []ParseResponse,resp base.Response)([]base.Data,[]error){
	if respParsers==nil {
		err:=errors.New("The response parser list is invalid")
		return nil,[]error{err}
	}
	httpResp:=resp.Httpresp()
	if httpResp==nil {
		err:=errors.New("The http response is invalid")
		return nil,[]error{err}
	}
	var reqUrl *url.URL=httpResp.Request.URL
	logger.Info("Parse the response (reqUrl=%s)... \n",reqUrl)
	respDepth:=resp.Depth()
	//解析Http响应
	dataList:=make([]base.Data,0)
	errorList:=make([]error,0)
	for i,respParser:= range respParsers {
		if respParser==nil {
			err:=errors.New(fmt.Sprintf("The document parser [%d] is invalid",i))
			errorList=append(errorList, err)
			continue
		}
		pDataList,pErrorList:=respParser(httpResp,respDepth)
		if pDataList!=nil {
			for _,pData:=range pDataList {
				dataList=appendDataList(dataList, pData,respDepth)
			}
		}
		if pErrorList!=nil {
			for _,pError:=range pErrorList {
				errorList=appendErrorList(errorList, pError)
			}
		}
	}
	return dataList,errorList
}
//添加请求值或条目到列表
func appendDataList(dataList []base.Data,data base.Data,respDepth uint32) []base.Data {
	if data==nil {
		return dataList
	}
	req,ok:=data.(*base.Request)
	if !ok {
		return append(dataList,data)
	}
	newDepth:=respDepth+1
	if req.Depth()!=newDepth {
		req=base.NewRequest(req.HttpReq(),newDepth)
	}
	return append(dataList,req)
}
//添加错误值到列表
func appendErrorList(errorList []error,err error)[]error{
	if err==nil {
		return errorList
	}
	return append(errorList, err)
}
//分析器池
type myAnalyzerPool struct{
	pool  middleware.Pool //实体池
	etype reflect.Type    //池内实体的类型
}
//生成网页下载器的函数类型
type GenAnalyzer func() Analyzer
//创建分析池
func NewAnalyzerPool(total uint32,gen GenAnalyzer)(AnalyzerPool, error)  {
	atype:=reflect.TypeOf(gen())
	genEntity:= func() middleware.Entity{return gen()}
	pool,err:= middleware.NewPool(total,atype,genEntity)
	if err!=nil {
		return nil,err
	}
	return myAnalyzerPool{pool,atype},nil
}
//从池中取出一个
func (apool myAnalyzerPool)Take()(Analyzer,error){
	entity,err:=apool.pool.Take()
	if err!=nil {
		return nil,err
	}
	an,ok:=entity.(Analyzer)//强制类型转换
	if !ok {
		errMsg:=fmt.Sprintf("The type of entity id NOT %s\n",apool.etype)
		panic(errors.New(errMsg))
	}
	return an,nil
}
//把一个归还给分析器
func (apool myAnalyzerPool)Return(analyzer Analyzer)error{
	return apool.pool.Return(analyzer)
}
//获得分析器的总容量
func (apool myAnalyzerPool)Total()uint32{
	return apool.pool.Total()
}
//获得正在使用的分析器数量
func (apool myAnalyzerPool)Used()uint32{
	return apool.pool.Used()
}