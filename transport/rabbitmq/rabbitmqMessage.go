package rabbitmq

import (
	"github.com/streadway/amqp"
)

// AMQP 消息实现
type RabbitMQMessage struct {
	message amqp.Delivery
	topic   string
}

func (am *RabbitMQMessage) GetBody() []byte {
	return am.message.Body
}

func (am *RabbitMQMessage) Ack() error {
	return am.message.Ack(false) // 确认消息
}

func (am *RabbitMQMessage) GetTopic() string {
	return am.topic
}
