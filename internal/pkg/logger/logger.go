package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LogOption struct {
	Format   string // 日志格式，console/json
	LogDir   string // 日志目录
	Level    string // 日志级别 debug/info/warn/error
	Compress bool   // 是否压缩旧日志
}

var (
	log      *zap.SugaredLogger
	raw      *zap.Logger
	initOnce sync.Once
)

// InitLogger 初始化全局 zap 日志器
func InitLogger(cfg LogOption) {
	initOnce.Do(func() {
		if cfg.LogDir == "" {
			cfg.LogDir = "logs"
		}
		if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
			panic(fmt.Sprintf("无法创建日志目录 %s: %v", cfg.LogDir, err))
		}

		// 固定使用东八区
		cstZone := time.FixedZone("CST", 8*3600)

		encoderCfg := zapcore.EncoderConfig{
			TimeKey:       "ts",
			LevelKey:      "level",
			NameKey:       "logger",
			CallerKey:     "caller",
			MessageKey:    "msg",
			StacktraceKey: "stacktrace",
			LineEnding:    zapcore.DefaultLineEnding,
			EncodeLevel:   zapcore.LowercaseLevelEncoder,
			EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(t.In(cstZone).Format("2006-01-02 15:04:05.000"))
			},
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}

		var encoder zapcore.Encoder
		switch cfg.Format {
		case "json":
			encoder = zapcore.NewJSONEncoder(encoderCfg)
		default:
			encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
			encoder = zapcore.NewConsoleEncoder(encoderCfg)
		}

		level := zapcore.InfoLevel
		if err := level.Set(cfg.Level); err != nil {
			panic(fmt.Sprintf("无效日志级别 %s，默认 info\n", cfg.Level))
		}

		infoLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
			return l < zapcore.WarnLevel && l >= level
		})
		errorLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
			return l >= zapcore.WarnLevel && l >= level
		})

		infoWriter := zapcore.Lock(zapcore.AddSync(&lumberjack.Logger{
			Filename:   filepath.Join(cfg.LogDir, "info.log"),
			MaxSize:    100,          // 每个日志文件最大100MB
			MaxBackups: 20,           // 最多保留10个旧日志
			MaxAge:     10,           // 最长保留 10 天
			Compress:   cfg.Compress, // 旧日志文件 gzip 压缩
		}))

		errorWriter := zapcore.Lock(zapcore.AddSync(&lumberjack.Logger{
			Filename:   filepath.Join(cfg.LogDir, "error.log"),
			MaxSize:    50,
			MaxBackups: 20,
			MaxAge:     10,
			Compress:   cfg.Compress,
		}))

		var cores []zapcore.Core
		cores = append(cores,
			zapcore.NewCore(encoder, infoWriter, infoLevel),
			zapcore.NewCore(encoder, errorWriter, errorLevel),
		)

		// 开发环境额外输出到控制台
		if cfg.Format == "console" {
			consoleCore := zapcore.NewCore(
				zapcore.NewConsoleEncoder(encoderCfg), // 编码器：把日志格式化成“控制台友好”的文本格式
				zapcore.AddSync(os.Stdout),            // 输出目标：标准输出（终端屏幕）
				level,
			)
			cores = append(cores, consoleCore)
		}

		core := zapcore.NewTee(cores...)
		raw = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.ErrorOutput(errorWriter)) // 注意这里
		log = raw.Sugar()
		zap.ReplaceGlobals(raw)
	})
}

// Sync 刷盘日志（程序退出前调用）
func Sync() {
	if raw != nil {
		_ = raw.Sync()
	}
}

// L 返回结构化 logger
func L() *zap.Logger {
	return raw
}

// S 返回 SugaredLogger
func S() *zap.SugaredLogger {
	return log
}

func Infof(format string, args ...any) {
	if log != nil {
		log.Infof(format, args...)
	}
}

func Warnf(format string, args ...any) {
	if log != nil {
		log.Warnf(format, args...)
	}
}

func Errorf(format string, args ...any) {
	if log != nil {
		log.Errorf(format, args...)
	}
}

func Debugf(format string, args ...any) {
	if log != nil {
		log.Debugf(format, args...)
	}
}

func Info(msg string, fields ...zap.Field) {
	if raw != nil {
		raw.Info(msg, fields...)
	}
}

func Error(msg string, fields ...zap.Field) {
	if raw != nil {
		raw.Error(msg, fields...)
	}
}

func Debug(msg string, fields ...zap.Field) {
	if raw != nil {
		raw.Debug(msg, fields...)
	}
}

func Warn(msg string, fields ...zap.Field) {
	if raw != nil {
		raw.Warn(msg, fields...)
	}
}
