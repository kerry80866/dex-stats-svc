package rest

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type SimpleRestServer struct {
	port   int
	server *http.Server
}

// NewSimpleRestServer 创建并返回一个新的 REST 服务实例
func NewSimpleRestServer(port int, routes map[string]http.HandlerFunc) *SimpleRestServer {
	mux := http.NewServeMux()

	// Prometheus Metrics 路由
	mux.Handle("/metrics", promhttp.Handler())

	// 注册自定义路由
	for path, handlerFunc := range routes {
		mux.HandleFunc(path, handlerFunc)
	}

	return &SimpleRestServer{
		port: port,
		server: &http.Server{
			Addr:    fmt.Sprintf("0.0.0.0:%d", port),
			Handler: mux,
		},
	}
}

// Start 启动 REST 服务
func (s *SimpleRestServer) Start() {
	go func() {
		logger.Infof("[SimpleRestServer] starting on port %d", s.port)
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("REST 服务启动失败: %v", err)
		}
	}()
}

// Stop 停止 REST 服务
func (s *SimpleRestServer) Stop() {
	logger.Infof("[SimpleRestServer] shutting down")
	_ = s.server.Shutdown(context.Background())
}
