package base

import (
	"errors"
	"fmt"
)

//参数容器接口
type Args interface {
	//自检参数的有效性，并在必要时返回可以说明问题的错误值
	//若结果值为nil则说明未发现问题，否则就意味着自检未通过
	Check()error
	//获得参数容器的字符串表现形式
	String()string
}
//通道参数容器
type ChannelArgs struct {
	reqChanLen uint//请求通道长度
	respChanLen uint//相应通道长度
	itemChanLen uint//条目通道长度
	errorChanLen uint//错误通道长度
	description string//描述
}
//池基本参数容器
type PoolBaseArgs struct {
	pageDownloaderPollSize uint32//网页下载池尺寸
	analyzerPollSize uint32//分析池尺寸
	description string//描述
}

func NewChannelArgs(reqChanLen,respChanLen,itemChanLen,errorChanLen uint) *ChannelArgs {
	return &ChannelArgs{reqChanLen:reqChanLen,respChanLen:respChanLen,itemChanLen:itemChanLen,errorChanLen:errorChanLen}
}
func NewPoolBaseArgs(pageDownloaderPollSize,analyzerPollSize uint32) *PoolBaseArgs {
	return &PoolBaseArgs{pageDownloaderPollSize:pageDownloaderPollSize,analyzerPollSize:analyzerPollSize}
}
//获得请求通道长度
func(args *ChannelArgs) ReqChanLen()uint  {
	return args.reqChanLen
}
//相应通道长度
func(args *ChannelArgs) RespChanLen()uint  {
	return args.respChanLen
}
//错误通道长度
func(args *ChannelArgs) ErrorChanLen()uint  {
	return args.errorChanLen
}
//条目通道长度
func(args *ChannelArgs) ItemChanLen()uint  {
	return args.itemChanLen
}
//若结果值为nil则说明未发现问题，否则就意味着自检未通过
func(args *ChannelArgs) Check()error  {
	if args.reqChanLen<=0 {
		return errors.New(" The reqChanLen must greater  0!\n")
	}
	if args.respChanLen<=0 {
		return errors.New(" The respChanLen must greater  0!\n")
	}
	if args.errorChanLen<=0 {
		return errors.New(" The errorChanLen must greater  0!\n")
	}
	if args.itemChanLen<=0 {
		return errors.New(" The itemChanLen must greater  0!\n")
	}
	return nil
}
//获得参数容器的字符串表现形式
func(args *ChannelArgs) String()string  {
	//reqChanLen uint//请求通道长度
	//respChanLen uint//相应通道长度
	//itemChanLen uint//条目通道长度
	//errorChanLen uint//错误通道长度
	args.description=fmt.Sprintf("reqChanLen: %d,respChanLen: %d,itemChanLen:%d ,errorChanLen : %d\n",args.reqChanLen,args.respChanLen,args.itemChanLen,args.errorChanLen)
	return args.description
}
//网页下载池尺寸
func(args *PoolBaseArgs) PageDownloaderPollSize()uint32  {
	return args.pageDownloaderPollSize
}
//分析池尺寸
func(args *PoolBaseArgs) AnalyzerPollSize()uint32  {
	return args.analyzerPollSize
}
//若结果值为nil则说明未发现问题，否则就意味着自检未通过
func(args *PoolBaseArgs)Check()error  {
	if args.pageDownloaderPollSize<=0 {
		return errors.New(" The pageDownloaderPollSize must greater  0!\n")
	}
	if args.analyzerPollSize<=0 {
		return errors.New(" The analyzerPollSize must greater  0!\n")
	}
	return nil
}
//获得参数容器的字符串表现形式
func(args *PoolBaseArgs) String()string   {
	//pageDownloaderPollSize uint32//网页下载池尺寸
	//analyzerPollSize uint32//分析池尺寸
	//description string//描述
	args.description=fmt.Sprintf("pageDownloaderPollSize: %d,analyzerPollSize: %d\n",args.pageDownloaderPollSize,args.analyzerPollSize)
	return args.description
}
//
//
//
//

