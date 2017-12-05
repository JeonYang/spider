package middleware

import (
	"sync"
	"math"
)

//Id生成器的接口类型
type IdGenertor interface {
	GetUint32() uint32//获取一个unit32类型的Id
}
type cyclicIdGenertor struct {
	id uint32//当前id
	ended bool//签一个id是否为其类型所能表示的做大值
	mutex sync.Mutex
}

func NewIdGenertor() IdGenertor {
	return &cyclicIdGenertor{}
}
//获取一个unit32类型的Id
func (gen *cyclicIdGenertor)GetUint32() uint32 {
	gen.mutex.Lock()
	defer gen.mutex.Unlock()
	if gen.ended {
		defer func() {gen.ended=false}()
		gen.id=uint32(1)
		return uint32(0)
	}
	id:=gen.id
	if  id<math.MaxInt32{
		gen.id++
	}else {
		gen.ended=true
	}
	return id
}
type cyclicIdGenertor2 struct {
	base cyclicIdGenertor//基本id生成器
	cycleCount uint64//基于unit32类型的取值范围的周期计数
}
//获取一个unit64类型的Id
func (gen *cyclicIdGenertor2)GetUint64() uint64{
	var id64 uint64
	if gen.cycleCount%2==1 {
		id64+=math.MaxUint32
	}
	id32:=gen.base.GetUint32()
	if id32==math.MaxInt32 {
		gen.cycleCount++
	}
	id64+=uint64(id32)
	return id64
}