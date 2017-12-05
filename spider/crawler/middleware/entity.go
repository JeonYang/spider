package middleware

import (
	"reflect"
	"fmt"
	"errors"
	"sync"
)

type Pool interface {
	Take()(Entity,error)//取出实体
	Return(entity Entity)(error)//归还实体
	Total()uint32//实体的容量
	Used()uint32//实体中已经被使用的实体数量
}
//实体的接口类型
type Entity interface {
	Id()uint32//ID的获取方法
}

//实体池的实现类型
type myPool struct {
	total uint32//池的总容量
	etype reflect.Type//池中实体的类型
	genEntity func()Entity//池中实体的生成函数
	container chan Entity//实体容器
	//实体Id的容器
	idContainer map[uint32]bool
	mutex sync.Mutex
}

func NewPool(total uint32,entityType reflect.Type,genEntity func()Entity)(Pool,error)  {
	if total==0 {
		errMsg:=fmt.Sprintf("The pool can not be initialized! (total=%d)\n",total)
		return nil,errors.New(errMsg)
	}
	size:=int(total)
	container:=make(chan Entity,size)
	idContainer:=make(map[uint32]bool)
	for i:=0;i<size ; i++ {
		newEntity:=genEntity()
		if entityType!=reflect.TypeOf(newEntity) {
			errMsg:=fmt.Sprintf("The type of result of function gen Entity()is Not %s\n",entityType)
			return nil,errors.New(errMsg)
		}
		container<-newEntity
		idContainer[newEntity.Id()]=true
	}
	pool:=&myPool{total,entityType,genEntity,container,idContainer,*new(sync.Mutex)}
	return pool,nil
}
//取出实体
func (pool *myPool)Take()(Entity,error){
	entity,ok:=<-pool.container
	if !ok {
		return nil,errors.New("The innercontainer is invalid")
	}
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.idContainer[entity.Id()]=false
	return  entity,nil
}
//归还实体
func (pool *myPool)Return(entity Entity)(error){
	if entity==nil {
		return errors.New("The returning entity is invalid")
	}
	if pool.etype!=reflect.TypeOf(entity) {
		errMsg:=fmt.Sprintf("The type of result of function gen Entity()is Not %s\n",pool.etype)
		return errors.New(errMsg)
	}
	entityId:=entity.Id()
	caseResult:=pool.compareAndSetForIdContainer(entityId,false,true)
	if caseResult==-1 {
		errMsg:=fmt.Sprintf("The entity(id=%d) is illegal!\n",entity.Id())
		return errors.New(errMsg)
	}
	if caseResult==0 {
		errMsg:=fmt.Sprintf("The entity(id=%d) is already in the pool!\n",entity.Id())
		return errors.New(errMsg)
	}else {
		pool.idContainer[entityId]=true
		pool.container<-entity
		return nil
	}
}
//比较并设置实体ID容器中与给定实体ID对应的键值对的元素值
//结果值;1操作成功
//		0.对应的id在容器中已经存在
//		-1.对应的id在容器中不存在
//
func (pool *myPool) compareAndSetForIdContainer(entityId uint32,oldValue,newValue bool)int8  {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	v,ok:=pool.idContainer[entityId]
	if !ok {
		return -1
	}
	if v!=oldValue {
		return 0
	}
	pool.idContainer[entityId]=newValue
	return 1
}
//实体的容量
func (pool *myPool)Total()uint32{
return uint32(cap(pool.container))
}
//实体中已经被使用的实体数量
func (pool *myPool)Used()uint32{

	return uint32(cap(pool.container)-len(pool.container))
}