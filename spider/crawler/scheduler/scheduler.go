package scheduler

import (
	"net/http"
	"spider/crawler/analyzer"
	"spider/crawler/itempipeline"
	"spider/crawler/middleware"
	"spider/crawler/downloader"
	"fmt"
	"github.com/Sirupsen/logrus"
	"spider/crawler/base"
	"errors"
	"sync/atomic"
	"time"
	"strings"
	"bytes"
	"regexp"
	"sync"
)

var logger *logrus.Logger= base.NewLogger()
//调度器的接口类型
type Scheduler interface {
//启动调动器
//调用该方法会使调动器和初始化各个组建。在此之后调度器会激活爬取流程的执行
//参数channelArgs被用来代表通道参数容器
//参数poolBaseArgs代表池基本参数容器
//参数crawDepth代表了需要被爬取的网页和最大深度值。深度大于此值的网页会被忽略
//参数httpClientGenerator代表的是被用来生成HTTP客户端的函数
//参数resqParsers的值应为分析器所需要的被用来解析Http相应的函数的序列
//参数itemProcessors的值应为需要被置入条目处理管道中的条目处理器序列
//参数firstHttpReq即代表首次请求。调度器会以此为起点开始执行爬取流程
	Start(channelArgs base.ChannelArgs,poolBaseArgs base.PoolBaseArgs,crawDepth uint32,httpClientGenerator GenHttpClient,resqParsers []analyzer.ParseResponse,itemProcessors []itempipeline.ProcessItem,firstHttpReq *http.Request)(err error)
	//调用该方法会停止调度器的运行。所有处理模块的流程都会被终止
	Stop()bool
	//判断调度器知否正在运行
	Runing()bool
	//获得错误通到。调度器以及各个处理模块运行过程中出现的所有错误都会被发送至该通道。
	//若该方法的结果值为nil，则说明错误通道不可用或者调度器已经被停止
	ErrorChan()<-chan error
	//判断所有处理模块是否都处于空闲状态
	Idle() bool
	//获取摘要信息。
	Summary(prefix string) SchedSummary
	}
//被用来生成HTTP客户端的函数类型
type GenHttpClient func()*http.Client

//调度器摘要信息的接口类型
type SchedSummary interface {
	String() string			//获取摘要信息的一般表示
	Detail() string			//获取摘要信息的详细表示
	Same(other SchedSummary)bool//判断是否与另一份摘要信息相同
}

type myScheduler struct {
	channelArgs base.ChannelArgs//channelArgs被用来代表通道参数容器
	poolBaseArgs base.PoolBaseArgs//poolBaseArgs代表池基本参数容器
	crawDepth uint32//爬取的最大深度
	primaryDomain string//主域名
	chanman middleware.ChannelManager//通道管理器
	stopSign middleware.StopeSign//停止信号
	dlPool downloader.PageDownloaderPoll//网页下载器池
	analyzerPool analyzer.AnalyzerPool//分析池
	itemPipeline itempipeline.ItemPipeline//条目处理管道
	runing uint32//运行标记，0表示未运行，1表示已运行，2表示已停止
	reqCache requestCache//请求缓存
	urlMap map[string]bool//已请求的url字典
	mutex sync.Mutex
}
type mySchedSummary struct {
	prefix string//前缀
	running uint32//运行标记
	poolBaseArgs base.PoolBaseArgs//poolBaseArgs代表池基本参数容器
	channelArgs base.ChannelArgs//channelArgs被用来代表通道参数容器
	crawDepth uint32//爬取的最大深度
	chanmanSummary string//通道管理器的摘要信息
	reqCacheSummary string//请求缓存的摘要信息
	itemPiplineSummary string//条目管道的摘要信息
	stopSignSummary string//停止信号的摘要信息
	dlPoolLen uint32//网页下载池的长度
	dlPoolCap uint32//网页下载池的容量
	analyzerPoolLen uint32//分析器池的长度
	analyzerPollCap uint32//分析器池的容量
	urlCount int//已请求的url计数
	urlDetail string//已请求的url的详细信息
}
//创建调度器
func NewScheduler()Scheduler  {
	return &myScheduler{}
}
//创建调度器摘要信息的接口类型
func NewSchedSummary(sched *myScheduler,prefix string) SchedSummary {
	if sched == nil {
		return nil
	}
	urlCount := len(sched.urlMap)
	var urlDetail string
	if urlCount > 0 {
		var buffer bytes.Buffer
		buffer.WriteByte('\n')
		sched.mutex.Lock()
		defer sched.mutex.Unlock()
		for k, _ := range sched.urlMap {
			buffer.WriteString(prefix)
			buffer.WriteString(prefix)
			buffer.WriteString(k)
			buffer.WriteByte('\n')
		}
		urlDetail = buffer.String()
	} else {
		urlDetail = "\n"
	}
	return &mySchedSummary{
		prefix:              prefix,
		running:             sched.runing,
		channelArgs:         sched.channelArgs,
		poolBaseArgs:        sched.poolBaseArgs,
		crawDepth:          sched.crawDepth,
		chanmanSummary:      sched.chanman.Summary(),
		reqCacheSummary:     sched.reqCache.summary(),
		dlPoolLen:           sched.dlPool.Used(),
		dlPoolCap:           sched.dlPool.Total(),
		analyzerPoolLen:     sched.analyzerPool.Used(),
		analyzerPollCap:     sched.analyzerPool.Total(),
		itemPiplineSummary: sched.itemPipeline.Sumary(),
		urlCount:            urlCount,
		urlDetail:           urlDetail,
		stopSignSummary:     sched.stopSign.Summary(),
	}
}
//启动调动器
//调用该方法会使调动器和初始化各个组建。在此之后调度器会激活爬取流程的执行
//参数channelLen被用来指定数据传输通道的长度
//参数pollSize被用来设定网页下载器和分析器的容量
//参数crawDepth代表了需要被爬取的网页和最大深度值。深度大于此值的网页会被忽略
//参数httpClientGenerator代表的是被用来生成HTTP客户端的函数
//参数resqParsers的值应为分析器所需要的被用来解析Http相应的函数的序列
//参数itemProcessors的值应为需要被置入条目处理管道中的条目处理器序列
//参数firstHttpReq即代表首次请求。调度器会以此为起点开始执行爬取流程
func (sched *myScheduler) Start(
	channelArgs base.ChannelArgs,
	poolBaseArgs base.PoolBaseArgs,
	crawlDepth uint32,
	httpClientGenerator GenHttpClient,
	respParsers []analyzer.ParseResponse,
	itemProcessors []itempipeline.ProcessItem,
	firstHttpReq *http.Request) (err error) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Scheduler Error: %s\n", p)
			logger.Fatal(errMsg)
			err = errors.New(errMsg)
		}
	}()
	if atomic.LoadUint32(&sched.runing) == 1 {
		return errors.New("The scheduler has been started!\n")
	}
	atomic.StoreUint32(&sched.runing, 1)

	if err := channelArgs.Check(); err != nil {
		return err
	}
	sched.channelArgs = channelArgs
	if err := poolBaseArgs.Check(); err != nil {
		return err
	}
	sched.poolBaseArgs = poolBaseArgs
	sched.crawDepth = crawlDepth

	sched.chanman = generateChannelManager(sched.channelArgs)
	if httpClientGenerator == nil {
		return errors.New("The HTTP client generator list is invalid!")
	}
	dlpool, err :=
		generatePageDownloaderPoll(
			sched.poolBaseArgs.PageDownloaderPollSize(),
			httpClientGenerator)
	if err != nil {
		errMsg :=
			fmt.Sprintf("Occur error when get page downloader pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.dlPool = dlpool
	analyzerPool, err := generateAnalyzerPoll(sched.poolBaseArgs.AnalyzerPollSize())
	if err != nil {
		errMsg :=
			fmt.Sprintf("Occur error when get analyzer pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.analyzerPool = analyzerPool

	if itemProcessors == nil {
		return errors.New("The item processor list is invalid!")
	}
	for i, ip := range itemProcessors {
		if ip == nil {
			return errors.New(fmt.Sprintf("The %dth item processor is invalid!", i))
		}
	}
	sched.itemPipeline =generateItemPipeLine(itemProcessors)

	if sched.stopSign == nil {
		sched.stopSign =middleware.NewStopeSign()
	} else {
		sched.stopSign.Reset()
	}

	sched.reqCache = newRequestCache()
	sched.urlMap = make(map[string]bool)

	sched.startDownloading()
	sched.activateAnalyzers(respParsers)
	sched.openItemPipline()
	sched.schedule(10 * time.Millisecond)

	if firstHttpReq == nil {
		return errors.New("The first HTTP request is invalid!")
	}
	pd, err := getPrimaryDomain(firstHttpReq.Host)
	if err != nil {
		return err
	}
	sched.primaryDomain = pd

	firstReq := base.NewRequest(firstHttpReq, 0)
	sched.reqCache.put(firstReq)

	return nil
}
//创建ChannelManager
func generateChannelManager(channelArgs base.ChannelArgs) middleware.ChannelManager {
	return middleware.NewChannelManager(channelArgs)
}
//创建下载池
func generatePageDownloaderPoll(poolSize uint32,httpClientGenerator GenHttpClient)(downloader.PageDownloaderPoll,error)  {
	dlPool, err := downloader.NewPageDownloaderPoll(
		poolSize,
		func() downloader.PageDownloader {
			return downloader.NewPageDownloader(httpClientGenerator())
		},
	)
	if err != nil {
		return nil, err
	}
	return dlPool, nil
}
//创建分析池
func generateAnalyzerPoll(poolSize uint32)(analyzer.AnalyzerPool, error)  {
	analyzerPool, err := analyzer.NewAnalyzerPool(
		poolSize,
		func() analyzer.Analyzer {
			return analyzer.NewAnalyzer()
		},
	)
	if err != nil {
		return nil, err
	}
	return analyzerPool, nil
}
//创建条目处理管道
func generateItemPipeLine(itemProcessors []itempipeline.ProcessItem) itempipeline.ItemPipeline {
	return itempipeline.NewItemPipeline(itemProcessors)
}
var regexpForIp = regexp.MustCompile(`((?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d))`)
var regexpForDomains = []*regexp.Regexp{
	// *.xx or *.xxx.xx
	regexp.MustCompile(`\.(com|com\.\w{2})$`),
	regexp.MustCompile(`\.(gov|gov\.\w{2})$`),
	regexp.MustCompile(`\.(net|net\.\w{2})$`),
	regexp.MustCompile(`\.(org|org\.\w{2})$`),
	// *.xx
	regexp.MustCompile(`\.me$`),
	regexp.MustCompile(`\.biz$`),
	regexp.MustCompile(`\.info$`),
	regexp.MustCompile(`\.name$`),
	regexp.MustCompile(`\.mobi$`),
	regexp.MustCompile(`\.so$`),
	regexp.MustCompile(`\.asia$`),
	regexp.MustCompile(`\.tel$`),
	regexp.MustCompile(`\.tv$`),
	regexp.MustCompile(`\.cc$`),
	regexp.MustCompile(`\.co$`),
	regexp.MustCompile(`\.\w{2}$`),
}
//获取主域名
func getPrimaryDomain(host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", errors.New("The host is empty!")
	}
	if regexpForIp.MatchString(host) {
		return host, nil
	}
	var suffixIndex int
	for _, re := range regexpForDomains {
		pos := re.FindStringIndex(host)
		if pos != nil {
			suffixIndex = pos[0]
			break
		}
	}
	if suffixIndex > 0 {
		var pdIndex int
		firstPart := host[:suffixIndex]
		index := strings.LastIndex(firstPart, ".")
		if index < 0 {
			pdIndex = 0
		} else {
			pdIndex = index + 1
		}
		return host[pdIndex:], nil
	} else {
		return "", errors.New("Unrecognized host!")
	}
}
//激活分析器
func(sched *myScheduler) activateAnalyzers(resqParsers []analyzer.ParseResponse)  {
	go func() {
		for   {
			resp,ok:=<-sched.getRespChan()
			if !ok {
				break
			}
			go sched.analyze(resqParsers,resp)
		}
	}()
}
//分析器
func (sched *myScheduler)analyze(resqParsers []analyzer.ParseResponse,resp base.Response){
	defer func() {
		if p:=recover();p!=nil {
			errMsg:=fmt.Sprintf("Fatal Analyze Error: %s\n",p)
			logger.Fatal(errMsg)
		}
	}()
	analyzer,err:=sched.analyzerPool.Take()
	if err!=nil {
		errMsg:=fmt.Sprintf("Analyzer pool error: %s",err)
		sched.sendError(errors.New(errMsg),SCHEDULER_CODE)
		return
	}
	defer func() {
		err:=sched.analyzerPool.Return(analyzer)
		if err!=nil {
			errMsg:=fmt.Sprintf("Analyzer pool error: %s",err)
			sched.sendError(errors.New(errMsg),SCHEDULER_CODE)
			return
		}
	}()
	code:=generateCode(ANALYZER_COOE,analyzer.Id())
	dataList,errs:=analyzer.Analuze(resqParsers,resp)
	if dataList!=nil {
		for _,data:=range dataList {
			if data==nil {
				continue
			}
			switch d:=data.(type) {
			case *base.Request:

				sched.saveReqToCach(*d,code)
			case *base.Item:
				sched.sendItem(*d,code)
			default:
				errMsg:=fmt.Sprintf("Unsupported data type '%T'! (value=%v)\n",d,d)
				sched.sendError(errors.New(errMsg),code)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			sched.sendError(err, code)
		}
	}
}
// 生成组件实例代号。
func generateCode(prefix string, id uint32) string {
	return fmt.Sprintf("%s-%d", prefix, id)
}

// 解析组件实例代号。
func parseCode(code string) []string {
	result := make([]string, 2)
	var codePrefix string
	var id string
	index := strings.Index(code, "-")
	if index > 0 {
		codePrefix = code[:index]
		id = code[index+1:]
	} else {
		codePrefix = code
	}
	result[0] = codePrefix
	result[1] = id
	return result
}
//将请求保存到请求缓存中
func (sched *myScheduler)saveReqToCach(req base.Request,code string)bool{
	httpReq:=req.HttpReq()
	if httpReq==nil {
		logger.Warnln("Ignore the request! It's request is inbalid!")
		return false
	}
	reqUrl:=httpReq.URL
	if reqUrl==nil{
		logger.Warnln("Ignore the request ! It's url is invalid!")
		return false
	}
	if strings.ToLower(reqUrl.Scheme)!="http"{
		logger.Warn("Ignore the request ! It's url schem '%s',but should be 'http'!\n",reqUrl.Scheme)
		return false
	}
	sched.mutex.Lock()
	defer sched.mutex.Unlock()
	if _, ok := sched.urlMap[reqUrl.String()]; ok {
		logger.Warnf("Ignore the request ! It's url is repeated. (requestUrl=%s)\n",reqUrl)
		return false
	}
	if pd,_:=getPrimaryDomain(httpReq.Host);pd!=sched.primaryDomain{
		logger.Warnf("Ignore the request ! It's host '%s' not in primary domain '%s'. ( (requestUrl=%s))\n",httpReq.Host,sched.primaryDomain,reqUrl)
		return false
	}
	if req.Depth()>sched.crawDepth {
		logger.Warnf("Ignore the request ! It's depth %d greater than %d.(requestUrl=%s)\n",req.Depth(),sched.crawDepth,reqUrl)
		return false
	}
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	sched.reqCache.put(&req)
	sched.urlMap[reqUrl.String()]=true
	return true
}
//打开条目通道
func (sched *myScheduler)openItemPipline(){
	go func() {
		sched.itemPipeline.SetFailFast(true)
		code:=ITEMPIELINE_COOE
		for item:=range sched.getItemChan(){
			go func(item base.Item) {
				defer func() {
					if  p:=recover();p!=nil{
						errMsg:=fmt.Sprintf("Fatal Item Processing Error: %s\n",p)
						logger.Fatal(errMsg)
					}
				}()
				errs:=sched.itemPipeline.Send(item)
				if errs!=nil {
					for _,err:=range errs {
						sched.sendError(err,code)
					}
				}
			}(item)
		}
	}()
}
//调度。适当的搬运请求缓存中的请求到请求通道中
func (sched *myScheduler)schedule( interval time.Duration){
	go func() {
		for {
			if sched.stopSign.Signed() {
				sched.stopSign.Deal(SCHEDULER_CODE)
				return
			}
			remainder := cap(sched.getReqchan()) - len(sched.getReqchan())
			var temp *base.Request
			for remainder > 0 {
				temp = sched.reqCache.get()
				if temp == nil {
					break
				}
				if sched.stopSign.Signed() {
					sched.stopSign.Deal(SCHEDULER_CODE)
					return
				}
				sched.getReqchan() <- *temp
				remainder--
			}
			time.Sleep(interval)
		}
	}()
}
//开始下载
func (sched *myScheduler)startDownloading(){
	go func() {
		for   {
			req,ok:=<-sched.getReqchan()
			if !ok {
				break
			}
			go sched.download(req)
		}
	}()
}
//获取通道管理器持有的请求通道
func (sched *myScheduler)getReqchan() chan base.Request{
	reqChan,err:=sched.chanman.ReqChan()
	if err!=nil {
		panic(err)
	}
	return reqChan
}
//获取通道管理器持有的响应通道
func (sched *myScheduler)getRespChan() chan base.Response{
	respChan,err:=sched.chanman.RespChan()
	if err!=nil {
		panic(err)
	}
	return respChan
}
//获取通道管理器持有的错误通道
func (sched *myScheduler)getErrorChan()  chan error{
	errChan,err:=sched.chanman.ErrorChan()
	if err!=nil {
		panic(err)
	}
	return errChan
}
//获取通道管理器持有的条目通道
func (sched *myScheduler)getItemChan()  chan base.Item{
	itemChan,err:=sched.chanman.ItemChan()
	if err!=nil {
		panic(err)
	}
	return itemChan
}
//组件的统一代号
const (
	DOWNLOADER_COOE="downloder"
	ANALYZER_COOE="analyzer"
	ITEMPIELINE_COOE="item_pipleine"
	SCHEDULER_CODE="scheduler"
)
//下载与请求相应的网络信息
func (sched *myScheduler)download(req  base.Request){

	defer func() {
		if p:=recover();p!=nil {
			errMsg:=fmt.Sprintf("Fatal Download Error: %s\n",p)
			logger.Fatal(errMsg)
		}
	}()
	downloader,err:=sched.dlPool.Take()
	if err!=nil {
		errMsg:=fmt.Sprintf("Downloader poll error: %s",err)
		sched.sendError(errors.New(errMsg),SCHEDULER_CODE)
		return
	}
	defer func() {
		err:=sched.dlPool.Return(downloader)
		if err!=nil {
			errMsg:=fmt.Sprintf("Downloader poll error: %s",err)
			sched.sendError(errors.New(errMsg),SCHEDULER_CODE)
		}
	}()
	code:=generateCode(DOWNLOADER_COOE,downloader.Id())
	respp,err:=downloader.Download(req)
	if err!=nil {
		sched.sendError(err,code)
	}
	if respp!=nil {
		sched.sendeResp(*respp,code)
	}
}
//发送请求
func (sched *myScheduler)sendeResp(resp base.Response,code string) bool{
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	sched.getRespChan()<-resp
	return true
}
//发送错误
func (sched *myScheduler)sendError(err error,code string)bool{
	if err==nil {
		return false
	}
	codePrefix:=parseCode(code)[0]
	var errType  base.ErrorType
	switch codePrefix {
	case DOWNLOADER_COOE:
		errType=base.DOWLOADER_ERROR
	case ANALYZER_COOE:
		errType=base.ANALYZER_ERROR
	case ITEMPIELINE_COOE:
		errType=base.ITEM_PROCESSOR_ERROR
	}
	cError:=base.NewCrawlerError(errType,err.Error())
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	go func() {
		sched.getErrorChan()<-cError
	}()
	return true
}
func (sched *myScheduler) sendItem(req base.Item,code string)  {

}
//调用该方法会停止调度器的运行。所有处理模块的流程都会被终止
func (sched *myScheduler)Stop()bool{
	if atomic.LoadUint32(&sched.runing)!=1 {
		return false
	}
	sched.stopSign.Sign()
	sched.chanman.Close()
	sched.reqCache.close()
	atomic.StoreUint32(&sched.runing,2)
	return true
}
//判断调度器知否正在运行
func (sched *myScheduler)Runing()bool{
	return  atomic.LoadUint32(&sched.runing)==1
}
//获得错误通到。调度器以及各个处理模块运行过程中出现的所有错误都会被发送至该通道。
//若该方法的结果值为nil，则说明错误通道不可用或者调度器已经被停止
func (sched *myScheduler)ErrorChan()<-chan error{
	if sched.chanman.Status()==middleware.CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}
	return sched.getErrorChan()
}
//判断所有处理模块是否都处于空闲状态
func (sched *myScheduler)Idle() bool{
	idleDlPool:=sched.dlPool.Used()==0
	idleAnalyzerPool:=sched.analyzerPool.Used()==0
	idleItemPipeline:=sched.itemPipeline.ProcrssingNumber()==0
	if idleDlPool&&idleAnalyzerPool&&idleItemPipeline {
		return true
	}
	return false
}

//获取摘要信息。
func (sched *myScheduler)Summary(prefix string) SchedSummary{
	return NewSchedSummary(sched,prefix)
}
//获取摘要信息
func (ss *mySchedSummary)getSummary(detail bool) string  {
	prefix:=ss.prefix
	template:=prefix+"Running: %v \n"+
		prefix+"Channel args: %s \n"+
		prefix+"Pool base args：%s \n"+
			prefix+"Crawl depth :%d \n"+
				prefix+"Channels manager: %s \n"+
					prefix+"Requst cache:%s \n"+
						prefix+"Downloader poll:%d/%d\n"+
							prefix+"Analizer poll:%d%d\n"+
								prefix+"Item pieline:%s\n"+
									prefix+"Urls(%d):%s"+
										prefix+"Stop singn:%s/n"

										return fmt.Sprintf(template, func() bool{return ss.running==1},ss.channelArgs.String(),ss.poolBaseArgs.String(),ss.crawDepth,ss.chanmanSummary,
										ss.reqCacheSummary,ss.dlPoolLen,ss.dlPoolCap,ss.analyzerPoolLen,ss.analyzerPollCap,ss.itemPiplineSummary,ss.urlCount,
											func() string{
												if detail {
													return ss.urlDetail
												}else {
													return "<concealed>\n"
												}
												},ss.stopSignSummary)
}
//获取摘要信息的一般表示
func (ss *mySchedSummary)String() string{
	return ss.getSummary(false)
}
//获取摘要信息的详细表示
func (ss *mySchedSummary)Detail() string{
	return ss.getSummary(true)
}
//判断是否与另一份摘要信息相同
func (ss *mySchedSummary)Same(other SchedSummary)bool{
	if other==nil {
		return false
	}
	otherSs,ok:=(other.(*mySchedSummary))
	if !ok {
		return false
	}
	if  ss.running!=otherSs.running||
		ss.poolBaseArgs!=otherSs.poolBaseArgs||
			ss.channelArgs!=otherSs.channelArgs||
				ss.crawDepth!=otherSs.crawDepth||
					ss.dlPoolLen!=otherSs.dlPoolLen||
						ss.dlPoolCap!=otherSs.dlPoolCap||
							ss.analyzerPoolLen!=otherSs.analyzerPoolLen||
								ss.analyzerPollCap!=otherSs.analyzerPollCap||
									ss.urlCount!=otherSs.urlCount||
										ss.stopSignSummary!=otherSs.stopSignSummary||
											ss.reqCacheSummary!=otherSs.reqCacheSummary||
												ss.itemPiplineSummary!=otherSs.itemPiplineSummary||
													ss.chanmanSummary!=otherSs.chanmanSummary {
		return false
	}else{
		return true
	}
}

