package kafka

// AMQP 消息实现
type KafkaMessage struct {
	message string
	topic   string
}

func (am *KafkaMessage) GetBody() []byte {
	return []byte(am.message)
}

func (am *KafkaMessage) Ack() error {
	return nil // 确认消息
}

func (am *KafkaMessage) GetTopic() string {
	return am.topic
}
