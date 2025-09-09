package handler

import (
	"dex-stats-sol/internal/stats"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func HealthCheck(app *stats.App) http.HandlerFunc {
	startTime := time.Now()
	return func(w http.ResponseWriter, r *http.Request) {
		// 使用 defer 和 recover 捕获 panic 错误
		defer func() {
			if r := recover(); r != nil {
				http.Error(w, fmt.Sprintf("Internal server error: %v", r), http.StatusInternalServerError)
			}
		}()

		// 超过 30 秒默认返回成功响应
		if time.Since(startTime) > 30*time.Second || app.IsReady() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := map[string]interface{}{
				"status":    "UP",
				"checkTime": formatLocalDateTime(),
				"details":   "Application is running normally (timeout exceeded)",
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		resp := map[string]interface{}{
			"status":  "DOWN",
			"details": "Application is not ready",
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// 格式化本地时间为 "yyyy-MM-ddTHH:mm:ss.SSSSSSS" 格式
func formatLocalDateTime() string {
	return time.Now().In(time.Local).Format("2006-01-02T15:04:05.9999999")
}
