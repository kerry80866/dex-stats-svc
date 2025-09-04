package utils

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/proto"
	"runtime/debug"
)

// SafeProtoUnmarshal 安全反序列化 Protobuf，防止 panic
func SafeProtoUnmarshal[T proto.Message](data []byte, msg T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in SafeProtoUnmarshal: %v\nstacktrace:\n%s", r, debug.Stack())
		}
	}()
	return proto.Unmarshal(data, msg)
}

// SafeProtoMarshal 安全序列化 Protobuf，防止 panic
func SafeProtoMarshal[T proto.Message](buf []byte, msg T) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in SafeProtoMarshal: %v\nstacktrace:\n%s", r, debug.Stack())
		}
	}()

	opts := proto.MarshalOptions{Deterministic: true}
	data, err = opts.MarshalAppend(buf, msg)
	return
}

// SafeJsonUnmarshal 安全反序列化 JSON，防止 panic
func SafeJsonUnmarshal[T any](data []byte, v *T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in SafeJsonUnmarshal: %v\nstacktrace:\n%s", r, debug.Stack())
		}
	}()
	return json.Unmarshal(data, v)
}

// SafeJsonMarshal 安全序列化 JSON，防止 panic
func SafeJsonMarshal[T any](v T) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in SafeJsonMarshal: %v\nstacktrace:\n%s", r, debug.Stack())
		}
	}()
	return json.Marshal(v)
}
