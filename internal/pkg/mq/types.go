package mq

type ConsumerState int32

const (
	StateStopped ConsumerState = iota
	StateStarting
	StateRunning
	StateStopping
)

func (s ConsumerState) String() string {
	switch s {
	case StateStopped:
		return "stopped"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}
