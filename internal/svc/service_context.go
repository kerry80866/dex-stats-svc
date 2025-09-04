package svc

import (
	"dex-stats-sol/internal/config"
	"dex-stats-sol/internal/pkg/nacos"
)

type ServiceContext struct {
	Cfg          *config.Config
	NacosManager *nacos.NacosManager
}

func NewServiceContext(c *config.Config) *ServiceContext {
	// 初始化 Nacos 客户端
	nacosManager, err := nacos.NewNacosManager(c.NacosConfig)
	if err != nil {
		panic(err)
	}

	return &ServiceContext{
		Cfg:          c,
		NacosManager: nacosManager,
	}
}
