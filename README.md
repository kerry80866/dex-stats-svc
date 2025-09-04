## 📁 项目目录结构说明（dex-stats-svc）

Solana DEX 指标统计与查询服务，具备高性能 TopN 排行、owner 活跃度统计、ticker 汇总等功能，模块清晰，职责分离。

---

### 📦 主要目录结构

| 目录路径                | 说明                                      |
|-------------------------|-----------------------------------------|
| `cmd/`                  | 服务启动入口，包含 `main.go`                     |
| `etc/`                  | 配置文件目录，使用 YAML 格式                       |
| `internal/config/`      | 配置结构体定义与加载逻辑                            |
| `internal/handler/`     | gRPC / HTTP 接口处理层，仅负责请求解析与转发            |
| `internal/logic/`       | 查询业务核心逻辑，例如排行榜构建、数据聚合等                  |
| `internal/service/`     | 后台异步服务（如 `totalSupply` 异步获取、ticker 写入等） |
| `internal/stats/`       | 核心模块，统一调度启动所有组件（类似主引擎）                  |
| `internal/svc/`         | 全局依赖注入容器（如 Redis、DB、Stats 等）            |
| `internal/server/`      | 启动并注册 gRPC / HTTP 服务                    |
| `internal/pkg/`         | 通用工具库（如 Kafka 封装、日志组件等）                 |
| `proto/`                | Protobuf 接口定义文件（`.proto`）               |
| `pb/`                   | 自动生成的 Protobuf Go 文件                    |

---

### ✅ 分层说明

| 模块层级  | 职责说明 |
|-----------|-----------|
| `handler` | 接收请求、调用 `logic`、处理异常；不包含任何业务逻辑 |
| `logic`   | 核心查询逻辑层，包含排行榜计算、分页、聚合、缓存命中等 |
| `service` | 异步后台逻辑，例如 ticker 刷新、owner 活跃度记录等 |
| `stats`   | 主流程协调模块（类似引擎），负责初始化、调度所有服务组件 |
| `svc`     | 全局依赖注入容器，持有 Redis、DB、Stats 实例等资源 |

---

