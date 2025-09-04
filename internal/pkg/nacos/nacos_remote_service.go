package nacos

import (
	"dex-stats-sol/internal/pkg/logger"
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"math/rand"
	"sync"
)

// NacosRemoteService 代表单个远程服务及其 gRPC 连接管理
type NacosRemoteService struct {
	serviceName string

	mu        sync.RWMutex
	instances []*model.Instance      // 当前健康实例列表
	clients   map[string]zrpc.Client // addr -> zrpc.Client
}

// NewNacosRemoteService 创建新的远程服务封装
func NewNacosRemoteService(serviceName string) *NacosRemoteService {
	return &NacosRemoteService{
		serviceName: serviceName,
		clients:     make(map[string]zrpc.Client),
	}
}

func (s *NacosRemoteService) instanceAddr(inst *model.Instance) string {
	return fmt.Sprintf("%s:%d", inst.Ip, inst.Port)
}

func (s *NacosRemoteService) UpdateInstances(instances []*model.Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldClients := s.clients
	s.clients = make(map[string]zrpc.Client)
	s.instances = instances

	// 保留仍存在的 client
	for _, inst := range instances {
		addr := s.instanceAddr(inst)
		if cli, ok := oldClients[addr]; ok {
			// 保留原有 client
			s.clients[addr] = cli
			delete(oldClients, addr)
			continue
		}

		// 新实例，直接创建 zrpc.Client
		newCli, err := zrpc.NewClient(zrpc.RpcClientConf{
			Target: addr,
		})
		if err != nil {
			logger.Errorf("failed to create zrpc client for %s: %v\n", addr, err)
			continue
		}
		s.clients[addr] = newCli
	}

	// oldClients 里剩下的是下线实例，直接丢弃（zrpc.Client 内部会自动回收）
}

// GetConn 返回一个随机实例的 gRPC 连接
func (s *NacosRemoteService) GetConn() (*grpc.ClientConn, error) {
	s.mu.RLock()
	n := len(s.instances)
	if n == 0 {
		s.mu.RUnlock()
		return nil, fmt.Errorf("no available instances for service %s", s.serviceName)
	}

	readyAddrs := make([]string, 0, n)
	for _, inst := range s.instances {
		addr := s.instanceAddr(inst)
		if cli, ok := s.clients[addr]; ok {
			if cli.Conn().GetState() == connectivity.Ready {
				readyAddrs = append(readyAddrs, addr)
			}
		} else {
			// 尚未创建 client，也算候选
			readyAddrs = append(readyAddrs, addr)
		}
	}

	readyLen := len(readyAddrs)
	if readyLen == 0 {
		s.mu.RUnlock()
		return nil, fmt.Errorf("no ready grpc connections for service %s", s.serviceName)
	}

	// 随机选择一个地址
	index := 0
	if readyLen > 1 {
		index = rand.Intn(readyLen)
	}
	addr := readyAddrs[index]

	if cli, ok := s.clients[addr]; ok {
		s.mu.RUnlock()
		return cli.Conn(), nil
	}
	s.mu.RUnlock()

	// 尝试获取或创建 client
	s.mu.Lock()
	defer s.mu.Unlock()
	if cli, ok := s.clients[addr]; ok {
		return cli.Conn(), nil
	}

	newCli, err := zrpc.NewClient(zrpc.RpcClientConf{Target: addr})
	if err != nil {
		return nil, fmt.Errorf("failed to create zrpc client for %s: %w", addr, err)
	}
	s.clients[addr] = newCli
	return newCli.Conn(), nil
}
