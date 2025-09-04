package nacos

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NacosServerConfig 仅服务端相关配置
type NacosServerConfig struct {
	ServiceName string `json:"service_name" yaml:"service_name"` // 注册到 Nacos 的服务名称
	GroupName   string `json:"group_name" yaml:"group_name"`     // 分组名称，默认 DEFAULT_GROUP
	Port        uint64 `json:"port" yaml:"port"`                 // gRPC 服务端口
	Weight      int    `json:"weight" yaml:"weight"`             // 权重，用于负载均衡
}

// NacosClientConfig 客户端订阅服务相关配置
type NacosClientConfig struct {
	ID          string `json:"id" yaml:"id"`                     // 内部标识，用于代码中快速引用
	ServiceName string `json:"service_name" yaml:"service_name"` // 需要订阅的服务名称
	GroupName   string `json:"group_name" yaml:"group_name"`     // 分组名称，默认 DEFAULT_GROUP
}

// NacosConfig Nacos 注册中心总配置
type NacosConfig struct {
	Server              *NacosServerConfig   `json:"server" yaml:"server"`                                   // 服务端注册信息
	Clients             []*NacosClientConfig `json:"clients" yaml:"clients"`                                 // 客户端订阅服务列表
	Username            string               `json:"username" yaml:"username"`                               // 连接 Nacos 用户名
	Password            string               `json:"password" yaml:"password"`                               // 连接 Nacos 密码
	TimeoutMs           int                  `json:"timeout_ms" yaml:"timeout_ms"`                           // 通信超时时间
	BeatIntervalMs      int                  `json:"beat_interval_ms" yaml:"beat_interval_ms"`               // 心跳间隔
	NamespaceId         string               `json:"namespace_id" yaml:"namespace_id"`                       // 命名空间
	NotLoadCacheAtStart bool                 `json:"not_load_cache_at_start" yaml:"not_load_cache_at_start"` // 启动是否读取本地缓存
	LogLevel            string               `json:"log_level" yaml:"log_level"`                             // 日志级别
	CacheDir            string               `json:"cache_dir" yaml:"cache_dir"`                             // 本地缓存目录
	LogDir              string               `json:"log_dir" yaml:"log_dir"`                                 // 日志文件目录
	Endpoint            string               `json:"endpoint" yaml:"endpoint"`                               // Nacos 云端地址
	StaticServers       []string             `json:"static_servers" yaml:"static_servers"`                   // 静态 Nacos 服务列表
}

type NacosManager struct {
	cfg                   *NacosConfig
	client                naming_client.INamingClient
	serviceMap            map[string]*NacosRemoteService
	mu                    sync.RWMutex // 保护 serviceMap
	nacosServerRegistered atomic.Bool
}

func NewNacosManager(cfg *NacosConfig) (*NacosManager, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	m := &NacosManager{
		cfg:        cfg,
		client:     client,
		serviceMap: make(map[string]*NacosRemoteService),
	}
	err = m.subscribeClients()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func newClient(cfg *NacosConfig) (naming_client.INamingClient, error) {
	var serverConfigs []constant.ServerConfig
	var endpoint string

	// 判断使用哪种模式
	if cfg.Endpoint != "" {
		endpoint = cfg.Endpoint
	} else if len(cfg.StaticServers) > 0 {
		serverConfigs = make([]constant.ServerConfig, 0, len(cfg.StaticServers))
		for _, s := range cfg.StaticServers {
			sc, err := parseStaticServer(s)
			if err != nil {
				return nil, err
			}
			serverConfigs = append(serverConfigs, sc)
		}
	} else {
		return nil, fmt.Errorf("either endpoint or static_servers must be configured")
	}

	clientConfig := constant.ClientConfig{
		Endpoint:            endpoint,
		NamespaceId:         cfg.NamespaceId,
		TimeoutMs:           uint64(cfg.TimeoutMs),
		NotLoadCacheAtStart: cfg.NotLoadCacheAtStart,
		LogDir:              cfg.LogDir,
		CacheDir:            cfg.CacheDir,
		LogLevel:            cfg.LogLevel,
		Username:            cfg.Username,
		Password:            cfg.Password,
		BeatInterval:        int64(cfg.BeatIntervalMs),
	}

	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  &clientConfig,
		ServerConfigs: serverConfigs,
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos naming client failed: %w", err)
	}
	return client, nil
}

func parseStaticServer(s string) (constant.ServerConfig, error) {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return constant.ServerConfig{}, fmt.Errorf("invalid static server format: %s", s)
	}

	ip := strings.TrimSpace(parts[0])
	portStr := strings.TrimSpace(parts[1])
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return constant.ServerConfig{}, fmt.Errorf("invalid port in static server %s: %w", s, err)
	}
	if port <= 0 || port > 65535 {
		return constant.ServerConfig{}, fmt.Errorf("port out of range in static server %s", s)
	}

	return constant.ServerConfig{
		IpAddr: ip,
		Port:   uint64(port),
		Scheme: "http",
	}, nil
}

func (m *NacosManager) IsServerRegistered() bool {
	return m.nacosServerRegistered.Load()
}

func (m *NacosManager) RegisterServer() error {
	if m.cfg.Server == nil || m.nacosServerRegistered.Load() {
		return nil
	}
	m.nacosServerRegistered.Store(true)

	ip, err := utils.GetLocalIP()
	if err != nil {
		m.nacosServerRegistered.Store(false)
		return fmt.Errorf("failed to get local IP: %w", err)
	}

	param := vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        m.cfg.Server.Port,
		ServiceName: m.cfg.Server.ServiceName,
		Weight:      float64(m.cfg.Server.Weight),
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		GroupName:   m.cfg.Server.GroupName,
	}

	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		success, registerErr := m.client.RegisterInstance(param)
		if registerErr == nil && success {
			logger.Infof("Successfully registered server instance: %s:%d", ip, m.cfg.Server.Port)
			return nil
		}

		if registerErr != nil {
			logger.Warnf("Attempt %d: failed to register server instance: %v", i+1, registerErr)
		} else {
			logger.Warnf("Attempt %d: register instance returned false", i+1)
		}

		time.Sleep(100 * time.Millisecond)
	}

	m.nacosServerRegistered.Store(false)
	return fmt.Errorf("failed to register server instance after %d attempts", maxRetries)
}

func (m *NacosManager) DeregisterServer() error {
	if m.cfg.Server == nil || !m.nacosServerRegistered.Load() {
		return nil
	}

	ip, err := utils.GetLocalIP()
	if err != nil {
		return fmt.Errorf("failed to get local IP: %w", err)
	}

	param := vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        m.cfg.Server.Port,
		ServiceName: m.cfg.Server.ServiceName,
		Ephemeral:   true,
	}

	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		success, deregisterErr := m.client.DeregisterInstance(param)
		if deregisterErr == nil && success {
			m.nacosServerRegistered.Store(false)
			logger.Infof("Successfully deregistered server instance: %s:%d", ip, m.cfg.Server.Port)
			return nil
		}

		if deregisterErr != nil {
			logger.Warnf("Attempt %d: failed to deregister server instance: %v", i+1, deregisterErr)
		} else {
			logger.Warnf("Attempt %d: deregister instance returned false", i+1)
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("failed to deregister server instance after %d attempts", maxRetries)
}

func (m *NacosManager) GetRemoteServiceByID(serviceID string) *NacosRemoteService {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nrs, ok := m.serviceMap[serviceID]
	if ok {
		return nrs
	}
	return nil
}

func (m *NacosManager) subscribeClients() error {
	for _, cli := range m.cfg.Clients {
		c := cli // capture for closure

		// 获取或创建 NacosRemoteService
		nrs := m.getOrCreateRemoteService(c.ID, c.ServiceName)

		// 构建订阅参数
		param := &vo.SubscribeParam{
			ServiceName: c.ServiceName,
			GroupName:   c.GroupName,
			SubscribeCallback: func(services []model.Instance, err error) {
				if err != nil {
					logger.Warnf("Subscribe callback error for service %s: %v", c.ServiceName, err)
					return
				}

				// 构建最新健康实例列表
				var instances []*model.Instance
				for _, inst := range services {
					if inst.Enable && inst.Healthy && inst.Weight > 0 {
						instCopy := inst
						instances = append(instances, &instCopy)
					}
				}

				// 更新远程服务实例
				nrs.UpdateInstances(instances)
				logger.Infof("Updated NacosRemoteService for %s: %d instances", c.ServiceName, len(instances))
			},
		}

		// 执行订阅
		if err := m.client.Subscribe(param); err != nil {
			logger.Errorf("Failed to subscribe service %s: %v", c.ServiceName, err)
			return err
		}
		logger.Infof("Subscribed service: %s", c.ServiceName)
	}
	return nil
}

func (m *NacosManager) getOrCreateRemoteService(serviceID, serviceName string) *NacosRemoteService {
	m.mu.RLock()
	nrs, ok := m.serviceMap[serviceID]
	m.mu.RUnlock()
	if ok {
		return nrs
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// double-check
	if nrs, ok = m.serviceMap[serviceID]; !ok {
		nrs = NewNacosRemoteService(serviceName)
		m.serviceMap[serviceID] = nrs
	}
	return nrs
}
