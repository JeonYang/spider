package base
//条目
type Item map[string]interface{}
//条目数据是否有效
func (item Item)Valid() bool {
	return item!=nil
}