package monitor

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

type MonitorServer struct {
	port   int
	server *http.Server
}

func NewMonitorServer(port int) *MonitorServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/", &ProfileHandler{})

	return &MonitorServer{
		port: port,
		server: &http.Server{
			Addr:    fmt.Sprintf("0.0.0.0:%d", port),
			Handler: mux,
		},
	}
}

func (m *MonitorServer) Start() {
	go func() {
		logger.Infof("[Monitor] starting on port %d", m.port)
		if err := m.server.ListenAndServe(); err != nil {
			logger.Errorf("监控服务启动失败: %v", err)
		}
	}()
}

func (m *MonitorServer) Stop() {
	logger.Infof("[Monitor] shutting down")
	_ = m.server.Shutdown(context.Background())
}

// =================== 处理健康检查 ===================

type ProfileHandler struct{}

func (s *ProfileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz", "/health/readiness", "/health/liveness":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		resp := map[string]interface{}{
			"status":    "UP",
			"checkTime": formatLocalDateTime(),
			"details":   "Application is running normally",
		}
		_ = json.NewEncoder(w).Encode(resp)
	default:
		http.DefaultServeMux.ServeHTTP(w, r)
	}
}

// 本地时间格式化函数，保持与 Java 一致
func formatLocalDateTime() string {
	return time.Now().In(time.Local).Format("2006-01-02T15:04:05.9999999")
}
