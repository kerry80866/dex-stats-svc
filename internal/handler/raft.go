package handler

import (
	"dex-stats-sol/internal/stats"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RaftNodeData struct {
	Node string `json:"node"`
}

func GetLeaderIP(app *stats.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 使用 defer 和 recover 捕获 panic 错误
		defer func() {
			if r := recover(); r != nil {
				http.Error(w, fmt.Sprintf("Internal server error: %v", r), http.StatusInternalServerError)
			}
		}()

		// 检查请求方法，支持 GET 和 POST 请求
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		// 获取 Raft 集群中的 Leader IP
		leaderIP, err := app.GetLeaderIP()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get leader IP: %v", err), http.StatusInternalServerError)
			return
		}

		// 返回成功的响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response := map[string]interface{}{
			"status":   "success",
			"leaderIP": leaderIP,
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

func AddRaftNode(app *stats.App) http.HandlerFunc {
	return handleRaftNodeChange(app, true)
}

func RemoveRaftNode(app *stats.App) http.HandlerFunc {
	return handleRaftNodeChange(app, false)
}

// 处理节点添加或删除的公共函数
func handleRaftNodeChange(app *stats.App, addNode bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 使用 defer 和 recover 捕获 panic 错误
		defer func() {
			if r := recover(); r != nil {
				http.Error(w, fmt.Sprintf("Internal server error: %v", r), http.StatusInternalServerError)
			}
		}()

		// 限制只允许 POST 请求
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		// 读取 r.Body 数据
		body, err := io.ReadAll(r.Body)
		defer r.Body.Close() // 确保请求体关闭
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		var data RaftNodeData
		err = json.Unmarshal(body, &data)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		// 调用 AddOrRemoveNode 方法
		err = app.AddOrRemoveNode(data.Node, addNode)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
			return
		}

		// 返回成功的响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response := map[string]interface{}{
			"status": "success",
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}
