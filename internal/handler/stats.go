package handler

import (
	"dex-stats-sol/internal/stats"
	"encoding/json"
	"fmt"
	"net/http"
)

func RecoveryAllStatsTasks(app *stats.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 使用 defer 和 recover 捕获 panic 错误
		defer func() {
			if r := recover(); r != nil {
				http.Error(w, fmt.Sprintf("Internal server error: %v", r), http.StatusInternalServerError)
			}
		}()

		recovered := app.StartRecoveryAllTasks()

		// 返回成功的响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response := map[string]interface{}{
			"recovered": recovered,
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}
