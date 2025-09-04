package logger

import (
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"go.uber.org/zap"
)

// ZapWriter 实现 logx.Writer 接口，将 logx 日志重定向至 zap
type ZapWriter struct{}

func (ZapWriter) Alert(v any) {
	zap.S().Errorw("ALERT", "message", toString(v))
}

func (ZapWriter) Close() error {
	return nil
}

func (ZapWriter) Debug(v any, fields ...logx.LogField) {
	zap.S().Debugw(toString(v), convertFields(fields)...)
}

func (ZapWriter) Error(v any, fields ...logx.LogField) {
	zap.S().Errorw(toString(v), convertFields(fields)...)
}

func (ZapWriter) Info(v any, fields ...logx.LogField) {
	zap.S().Infow(toString(v), convertFields(fields)...)
}

func (ZapWriter) Severe(v any) {
	zap.S().Fatalw("SEVERE", "message", toString(v))
}

func (ZapWriter) Slow(v any, fields ...logx.LogField) {
	zap.S().Warnw("SLOW "+toString(v), convertFields(fields)...)
}

func (ZapWriter) Stack(v any) {
	zap.S().Errorw("STACK", "stack", toString(v))
}

func (ZapWriter) Stat(v any, fields ...logx.LogField) {
	zap.S().Infow("STAT", append([]any{"summary", toString(v)}, convertFields(fields)...)...)
}

func convertFields(fields []logx.LogField) []any {
	out := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		out = append(out, f.Key, f.Value)
	}
	return out
}

func toString(v any) string {
	if str, ok := v.(string); ok {
		return str
	}
	return fmt.Sprintf("%+v", v)
}
