package types

// Serializable 定义了最小序列化接口
type Serializable interface {
	Serialize(buf []byte) ([]byte, error)
}
