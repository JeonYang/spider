package base

import (
	"github.com/Sirupsen/logrus"
)

func NewLogger()*logrus.Logger  {
	return  logrus.New()
}