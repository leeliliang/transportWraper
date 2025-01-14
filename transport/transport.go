package transport

import (
	"sync"
)

var (
	transportInstance *UnifiedTransport
	transportOnce     sync.Once
)

// Transport 接口定义了传输层的基本操作
type Transport interface {
	Read() (UnificationMessage, error)
	Write(p []byte, topic, routerKey string) error
	Close()
}

// UnifiedTransport 统一的传输结构体
type UnifiedTransport struct {
	senders  map[string]Transport
	receives map[string]Transport
	msgChan  chan UnificationMessage
	mu       sync.Mutex
}

// GetUnifiedTransport NewUnifiedTransport 创建一个新的 UnifiedTransport 实例
func GetUnifiedTransport() *UnifiedTransport {
	return transportInstance
}

// NewUnifiedTransport 创建一个新的 UnifiedTransport 实例
func NewUnifiedTransport() *UnifiedTransport {
	transportOnce.Do(func() {
		transportInstance = &UnifiedTransport{
			senders:  make(map[string]Transport),
			receives: make(map[string]Transport),
			msgChan:  make(chan UnificationMessage, 1000),
		}
	})
	return transportInstance
}

func (u *UnifiedTransport) AddSender(topic string, t Transport) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.senders[topic] = t
}

func (u *UnifiedTransport) AddReceiver(topic string, t Transport) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.receives[topic] = t
	go func() {
		for {
			msg, readErr := t.Read()
			if readErr != nil {
				continue
			}
			u.msgChan <- msg
		}
	}()
}

// Read 从传输层读取数据
func (u *UnifiedTransport) Read() (UnificationMessage, error) {
	select {
	case msg := <-u.msgChan:
		//rt.logger.Info("Read message", zap.String("topic", responseMessage.Topic), zap.String("msg", string(responseMessage.Msg)))
		return msg, nil
	}
}

// Write 向传输层写入数据
func (u *UnifiedTransport) Write(p []byte, topic, routerKey string) error {
	transport, exist := u.senders[topic]
	if exist {
		err := transport.Write(p, topic, routerKey)
		if err != nil {

		}
	}
	return nil
}

// Close 关闭传输层
func (u *UnifiedTransport) Close() {

}
