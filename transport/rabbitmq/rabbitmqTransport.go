package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/leeliliang/transportWraper/transport"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

// RabbitMQTransport 管理 RabbitMQ 客户端、多个消费者和生产者
type TransportRabbitMQ struct {
	amqpURI   string
	conn      *amqp.Connection
	channel   *amqp.Channel
	senders   map[string]string
	receivers map[string]<-chan amqp.Delivery
	msgChan   chan amqp.Delivery
	errorChan chan error
	logger    *zap.Logger
	prefix    string

	channelChan chan error
	reConnect   bool
}

// NewTransportRabbitMQ 创建一个新的 TransportRabbitMQ
func NewTransportRabbitMQ(amqpURI, prefix string, logger *zap.Logger) (*TransportRabbitMQ, error) {
	msgChan := make(chan amqp.Delivery, 1000)
	transportRabbitMQ := &TransportRabbitMQ{
		amqpURI:   amqpURI,
		senders:   make(map[string]string),
		receivers: make(map[string]<-chan amqp.Delivery),
		msgChan:   msgChan,
		errorChan: make(chan error),
		reConnect: false,
		logger:    logger,
		prefix:    prefix,
	}

	err := transportRabbitMQ.Connect()
	if err != nil {
		logger.Error("Connect error", zap.Error(err))
		return nil, err
	}

	return transportRabbitMQ, nil

}

func (rt *TransportRabbitMQ) Connect() error {
	var err error
	dialConfig := amqp.Config{
		Properties: amqp.Table{
			"connection_timeout": int32(3 * time.Second / time.Millisecond)},
		Heartbeat: 10 * time.Second,
	}

	for {
		rt.conn, err = amqp.DialConfig(rt.amqpURI, dialConfig)
		if err == nil {
			rt.logger.Info("Connected to RabbitMQ, break")
			break // 成功连接，退出循环
		}
		// 连接失败，打印错误信息并等待 3 秒后重试
		rt.logger.Error("Error in creating RabbitMQ connection", zap.Error(err))
		time.Sleep(3 * time.Second) // 等待 3 秒后重试
	}

	rt.logger.Info("Connected to RabbitMQ")
	go func() {
		<-rt.conn.NotifyClose(make(chan *amqp.Error))
		//Listen to NotifyClose
		rt.logger.Info("go func() Connection Closed")
		rt.errorChan <- errors.New("Connection Closed")
	}()

	go func() {
		rt.logger.Info("Start consuming")
		if err := <-rt.errorChan; err != nil {
			rt.reConnect = true
			rt.logger.Info("Start reconnect consuming")
			time.Sleep(1 * time.Second)
			err := rt.Reconnect()
			if err != nil {
				rt.logger.Error("Reconnect error", zap.Error(err))
				return
			}
			rt.logger.Info("Reconnecting")
		}
		rt.logger.Info("Reconnecting finish")
	}()

	rt.channel, err = rt.conn.Channel()
	if err != nil {
		rt.logger.Error("Channel error", zap.Error(err))
		return fmt.Errorf("Channel: %s", err)
	}

	return nil
}

// Reconnect 重新连接 RabbitMQ 并重新声明交换机和消费者
func (rt *TransportRabbitMQ) Reconnect() error {
	rt.logger.Info("Reconnect")
	rt.Close()
	rt.logger.Info("c.close")
	if err := rt.Connect(); err != nil {
		rt.logger.Info("c.Start(); err != nil")
		return err
	}

	for exchange := range rt.senders {
		if err := rt.AddSender(exchange); err != nil {
			rt.logger.Error("AddSender error", zap.Error(err))
			return err
		}
	}

	for exchange := range rt.receivers {
		if err := rt.AddReceiver(exchange); err != nil {
			rt.logger.Error("AddReceiver error", zap.Error(err))
			return err
		}
	}

	return nil
}

func (rt *TransportRabbitMQ) AddSender(exchange string) error {
	err := rt.channel.ExchangeDeclare(
		exchange, // name
		"fanout", // kind
		false,    // durable
		false,    // autoDelete
		false,    // internal
		false,    // noWait
		nil,      // args
	)
	if err != nil {
		rt.logger.Info("ExchangeDeclare error", zap.Error(err))
		return err
	}
	rt.senders[exchange] = exchange
	return nil
}

func (rt *TransportRabbitMQ) AddReceiver(exchange string) error {
	queue := rt.prefix + transport.GenerateUUID()
	err := rt.channel.ExchangeDeclare(
		exchange, // name
		"fanout", // kind
		false,    // durable
		false,    // autoDelete
		false,    // internal
		false,    // noWait
		nil,      // args
	)
	if err != nil {
		rt.logger.Info("ExchangeDeclare error", zap.Error(err))
		return err
	}

	rt.logger.Info("BindQueue", zap.String("queue", exchange), zap.String("queue", queue))
	if _, err := rt.channel.QueueDeclare(queue, true, true, false, false, nil); err != nil {
		rt.logger.Error("error in declaring the queue", zap.Error(err))
		return fmt.Errorf("error in declaring the queue %s", err)
	}

	if err := rt.channel.QueueBind(queue, queue, exchange, false, nil); err != nil {
		rt.logger.Error("Queue  Bind error", zap.Error(err))
		return fmt.Errorf("Queue  Bind error: %s", err)
	}

	deliveries, err := rt.channel.Consume(queue, queue, false, false, false, false, nil)
	if err != nil {
		rt.logger.Error("Consume error", zap.Error(err))
		return err
	}
	go func(msg <-chan amqp.Delivery) {
		for {
			select {
			case m, ok := <-msg:
				if !ok {
					rt.logger.Info("Channel closed", zap.Any("msg", m), zap.Any("ok", ok))
					rt.Close()
					return
				}
				if rt.reConnect && string(m.Body) == "" {
					rt.logger.Error("connection closed")
					return
				}
				rt.logger.Info("Read message", zap.String("msg", string(m.Body)))
				rt.msgChan <- m
			}
		}
	}(deliveries)
	return nil
}

func (rt *TransportRabbitMQ) AddConsistentSender(exchange string) error {
	err := rt.channel.ExchangeDeclare(
		exchange,            // name
		"x-consistent-hash", // kind
		true,                // durable
		true,                // autoDelete
		false,               // internal
		false,               // noWait
		nil,                 // args
	)
	if err != nil {
		rt.logger.Info("ExchangeDeclare error", zap.Error(err))
		return err
	}
	rt.senders[exchange] = exchange
	return nil
}

func (rt *TransportRabbitMQ) AddConsistentReceiver(exchange string) error {
	queue := rt.prefix + transport.GenerateUUID()
	err := rt.channel.ExchangeDeclare(
		exchange,            // name
		"x-consistent-hash", // kind
		true,                // durable
		true,                // autoDelete
		false,               // internal
		false,               // noWait
		nil,                 // args
	)
	if err != nil {
		rt.logger.Info("ExchangeDeclare error", zap.Error(err))
		return err
	}

	rt.logger.Info("BindQueue", zap.String("queue", queue), zap.String("exchange", exchange))
	if _, err := rt.channel.QueueDeclare(queue, true, true, false, false, nil); err != nil {
		rt.logger.Error("error in declaring the queue", zap.Error(err))
		return fmt.Errorf("error in declaring the queue %s", err)
	}

	if err := rt.channel.QueueBind(queue, "1", exchange, false, nil); err != nil {
		rt.logger.Error("Queue  Bind error", zap.Error(err))
		return fmt.Errorf("Queue  Bind error: %s", err)
	}

	deliveries, err := rt.channel.Consume(queue, queue, false, false, false, false, nil)
	if err != nil {
		rt.logger.Error("Consume error", zap.Error(err))
		return err
	}
	go func(msg <-chan amqp.Delivery) {
		for {
			select {
			case m, ok := <-msg:
				if !ok {
					rt.logger.Info("Channel closed", zap.Any("msg", m), zap.Any("ok", ok))
					rt.Close()
					return
				}
				if rt.reConnect && string(m.Body) == "" {
					rt.logger.Error("connection closed")
					return
				}
				rt.logger.Info("Read message", zap.String("msg", string(m.Body)))
				rt.msgChan <- m
			}
		}
	}(deliveries)
	return nil
}

// Read 从消息通道中读取一条消息
func (rt *TransportRabbitMQ) Read() (transport.UnificationMessage, error) {
	select {
	case msg := <-rt.msgChan:
		rabbitMQMessage := RabbitMQMessage{message: msg, topic: msg.Exchange}
		//rt.logger.Info("Read message", zap.String("topic", responseMessage.Topic), zap.String("msg", string(responseMessage.Msg)))
		return &rabbitMQMessage, nil
	}
}

// Write 向 RabbitMQ 发送消息
func (rt *TransportRabbitMQ) Write(msg []byte, exchange, routerKey string) error {
	rt.logger.Info("Write message", zap.String("exchange", exchange), zap.String("msg", string(msg)))

	p := amqp.Publishing{
		Headers:     amqp.Table{"type": "text/plain"},
		ContentType: "text/plain",
		Body:        msg,
	}

	if _, exists := rt.senders[exchange]; exists {
		if err := rt.channel.Publish(exchange, routerKey, false, false, p); err != nil {
			rt.logger.Error("Error in Publishing", zap.Error(err))
			return fmt.Errorf("Error in Publishing: %s", err)
		}
	}
	return nil
}

// Close 关闭 TransportRabbitMQ
func (rt *TransportRabbitMQ) Close() {
	err := rt.conn.Close()
	if err != nil {
		rt.logger.Error("Close error", zap.Error(err))
		return
	}
}
