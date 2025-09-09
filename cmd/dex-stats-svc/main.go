package main

import (
	"dex-stats-sol/internal/config"
	"dex-stats-sol/internal/handler"
	"dex-stats-sol/internal/pkg/configloader"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/rest"
	"dex-stats-sol/internal/stats"
	"dex-stats-sol/internal/svc"
	"dex-stats-sol/pb"
	"flag"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	zerosvc "github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

var configFile = flag.String("f", "etc/dex-stats-svc/test.yaml", "the config file")

func main() {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic: %+v\nstack: %s", r, debug.Stack())
		}
	}()
	defer logger.Sync()

	flag.Parse()
	logger.Infof("Loading config from %s", *configFile)

	// 加载配置
	var c config.Config
	if err := configloader.LoadConfig(*configFile, &c); err != nil {
		panic(fmt.Sprintf("配置加载失败: %v", err))
	}

	// 初始化 zap 日志
	logger.InitLogger(c.LogConf.ToLogOption())
	logx.SetWriter(logger.ZapWriter{})

	// 初始化依赖注入上下文
	svcCtx := svc.NewServiceContext(&c)

	// 构造 go-zero ServiceGroup 管理服务
	sg := zerosvc.NewServiceGroup()

	// 构建并注册 gRPC 服务
	app := stats.NewApp(svcCtx)
	rpcServer := zrpc.MustNewServer(c.Grpc.ToRpcServerConf(), func(grpcServer *grpc.Server) {
		pb.RegisterStatsQueryServiceServer(grpcServer, app)
	})
	sg.Add(app)
	sg.Add(rpcServer)

	// 构建 rest 服务
	sg.Add(initializeRestServer(&c, app))

	// 启动服务
	logger.Infof("stats starting")
	sg.Start()

	// 等待退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down services...")
	sg.Stop()
}

func initializeRestServer(c *config.Config, app *stats.App) *rest.SimpleRestServer {
	healthCheck := handler.HealthCheck(app)
	routes := map[string]http.HandlerFunc{
		"/raft/leader":      handler.GetLeaderIP(app),
		"/healthz":          healthCheck,
		"/health/readiness": healthCheck,
		"/health/liveness":  healthCheck,
	}

	if c.ServerConf.AllowChangeRaftNode {
		routes["/raft/add_node"] = handler.AddRaftNode(app)
		routes["/raft/remove_node"] = handler.RemoveRaftNode(app)
	}

	return rest.NewSimpleRestServer(c.ServerConf.Port, routes)
}
