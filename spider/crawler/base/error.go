package base

import (
	"bytes"
	"fmt"
)

type ErrorType  string

type CrawlerError interface {
	Type() ErrorType//错误类型
	Error() string//获得错误提示信息
}
type myCrawlerError struct {
	errorType	ErrorType//错误类型
	errMsg string//错误提示信息
	fullErrMsg string//完整的错误提示信息
}

const (
	DOWLOADER_ERROR ErrorType="Dowload Error"//下载错误
	ANALYZER_ERROR ErrorType="Analyzer Error"//分析器错误
	ITEM_PROCESSOR_ERROR ErrorType="Item Processor Error"//条目错误
)
//创建爬虫错误
func NewCrawlerError(errorType ErrorType,ErrMsg string) CrawlerError {
	
	return  &myCrawlerError{errorType:errorType,errMsg:ErrMsg}
}
//错误类型
func (ce *myCrawlerError) Type() ErrorType {
	return ce.errorType
}
//错误提示信息
func (ce *myCrawlerError) Error() string {
	if ce.fullErrMsg=="" {
		ce.genFullErrMsg()
	}
	return ce.fullErrMsg
}
//生成错误提示心思
func (ce *myCrawlerError) genFullErrMsg()  {
	var buffer bytes.Buffer
	buffer.WriteString("Crauler Error:")
	if ce.errorType!="" {
		buffer.WriteString(string(ce.errorType))
		buffer.WriteString(":")
	}
	buffer.WriteString(ce.errMsg)
	ce.fullErrMsg=fmt.Sprintf("%s\n",buffer.String())
	return
}
