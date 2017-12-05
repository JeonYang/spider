package base

import (
	"github.com/Sirupsen/logrus"
)

// 创建日志记录器。
func NewLogger()*logrus.Logger  {
	return  logrus.New()
}
