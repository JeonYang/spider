package downloader

import (
	"spider/crawler/base"
	"net/http"
	"spider/crawler/middleware"
	"reflect"
	"fmt"
	"errors"
	"github.com/Sirupsen/logrus"
)
var logger *logrus.Logger= base.NewLogger()
//网页下载器的接口类型
type PageDownloader interface {
	Id() uint32//获取Id
	Download(req base.Request)(*base.Response,error)//根据请求下载网页，并返回响应
}

//网页下载器池的接口类型
type PageDownloaderPoll interface {
	Take()(PageDownloader,error)//从池中取出一个网页下载器
	Return(del PageDownloader) error//把一个网页下载器归还给他
	Total()uint32//获得池的容量
	Used()uint32//获得正在被使用的网页下载器的数量
}
//Id生成器
var downloaderIdGenertor middleware.IdGenertor = middleware.NewIdGenertor()
//页面下载器的实现类
type myPageDownloader struct{
	httpClient *http.Client//http客户端
	id uint32//ID
}
//生成并返回id
func genDownloaderId() uint32 {
	return downloaderIdGenertor.GetUint32()
}
//创建页面下载器
func NewPageDownloader(client *http.Client) PageDownloader {
	id:=genDownloaderId()
	if client==nil {
		client=&http.Client{}
	}
	return &myPageDownloader{client,id}
}
//获取id
func (dl myPageDownloader)Id() uint32 {
	return dl.id
}
//根据请求下载网页，并返回响应
func (dl myPageDownloader)Download(req base.Request)(*base.Response,error) {
	httpreq:=req.HttpReq()
	httpresp,err:=dl.httpClient.Do(httpreq)
	if err!=nil {
		return nil,err
	}
	return base.NewResponse(httpresp,req.Depth()),nil
}
//网络下载池的实现类型
type myDownloaderPoll struct {
	pool  middleware.Pool //实体池
	etype reflect.Type    //池内实体的类型
}
//生成网页下载器的函数类型
type GenPageDownloader func() PageDownloader
//创建网页下载器池
func NewPageDownloaderPoll(total uint32,gen GenPageDownloader) (PageDownloaderPoll,error) {
	etype:=reflect.TypeOf(gen())
	genEntity:= func() middleware.Entity{return gen()}
	pool,err:= middleware.NewPool(total,etype,genEntity)
	if err!=nil {
		return nil,err
	}
	dlpool:=&myDownloaderPoll{pool,etype}
	return dlpool,nil
}
//从池中取出一个网页下载器
func (blpool myDownloaderPoll)Take()(PageDownloader,error) {
	entity,err:=blpool.pool.Take()
	if err!=nil {
		return nil,err
	}
	dl,ok:=entity.(PageDownloader)//强制类型转换
	if !ok {
		errMsg:=fmt.Sprintf("The type of entity id NOT %s\n",blpool.etype)
		panic(errors.New(errMsg))
	}
	return dl,nil
}
//把一个网页下载器归还给他
func (blpool myDownloaderPoll)Return(del PageDownloader) error{
	return blpool.pool.Return(del)
}
//获得池的容量
func (blpool myDownloaderPoll)Total()uint32{
	return blpool.pool.Total()
}
//获得正在被使用的网页下载器的数量
func (blpool myDownloaderPoll)Used()uint32{
	return blpool.pool.Used()
}