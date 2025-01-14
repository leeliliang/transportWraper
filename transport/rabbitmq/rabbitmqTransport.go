package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/leeliliang/transportWraper/transport"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

type TransportRabbitMQ struct {
	amqpURI             string
	conn                *amqp.Connection
	channel             *amqp.Channel
	senders             map[string]string
	receivers           map[string]<-chan amqp.Delivery
	consistentSenders   map[string]string
	consistentReceivers map[string]<-chan amqp.Delivery
	msgChan             chan amqp.Delivery
	errorChan           chan error
	logger              *zap.Logger
	prefix              string
	reConnect           bool
}

// NewTransportRabbitMQ 创建一个新的 TransportRabbitMQ
func NewTransportRabbitMQ(amqpURI, prefix string, logger *zap.Logger) (*TransportRabbitMQ, error) {
	transportRabbitMQ := &TransportRabbitMQ{
		amqpURI:   amqpURI,
		senders:   make(map[string]string),
		receivers: make(map[string]<-chan amqp.Delivery),
		msgChan:   make(chan amqp.Delivery, 1000),
		errorChan: make(chan error),
		reConnect: false,
		logger:    logger,
		prefix:    prefix,
	}

	if err := transportRabbitMQ.Connect(); err != nil {
		logger.Error("Connect error", zap.Error(err))
		return nil, err
	}

	go transportRabbitMQ.monitorConnection()
	return transportRabbitMQ, nil
}

func (rt *TransportRabbitMQ) Connect() error {
	var err error
	dialConfig := amqp.Config{
		Properties: amqp.Table{"connection_timeout": int32(3 * time.Second / time.Millisecond)},
		Heartbeat:  10 * time.Second,
	}

	for {
		if rt.conn, err = amqp.DialConfig(rt.amqpURI, dialConfig); err == nil {
			rt.logger.Info("Connected to RabbitMQ")
			break
		}
		rt.logger.Error("Error in creating RabbitMQ connection", zap.Error(err))
		time.Sleep(3 * time.Second)
	}

	rt.channel, err = rt.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %w", err)
	}

	return nil
}

func (rt *TransportRabbitMQ) monitorConnection() {
	<-rt.conn.NotifyClose(make(chan *amqp.Error))
	rt.logger.Info("Connection Closed")
	rt.errorChan <- errors.New("Connection Closed")

	rt.reConnect = true
	time.Sleep(1 * time.Second)
	if err := rt.Reconnect(); err != nil {
		rt.logger.Error("Reconnect error", zap.Error(err))
	}
}

func (rt *TransportRabbitMQ) Reconnect() error {
	rt.logger.Info("Reconnect")
	rt.Close()
	if err := rt.Connect(); err != nil {
		return err
	}

	for exchange := range rt.senders {
		if err := rt.AddSender(exchange); err != nil {
			return err
		}
	}

	for exchange := range rt.consistentSenders {
		if err := rt.AddConsistentSender(exchange); err != nil {
			return err
		}
	}

	for exchange := range rt.receivers {
		if err := rt.AddReceiver(exchange); err != nil {
			return err
		}
	}

	for exchange := range rt.consistentReceivers {
		if err := rt.AddConsistentReceiver(exchange); err != nil {
			return err
		}
	}

	return nil
}

func (rt *TransportRabbitMQ) AddSender(exchange string) error {
	return rt.declareExchange(exchange, "fanout")
}

func (rt *TransportRabbitMQ) AddReceiver(exchange string) error {
	queue := rt.prefix + transport.GenerateUUID()
	if err := rt.declareExchange(exchange, "fanout"); err != nil {
		return err
	}

	if err := rt.bindQueue(queue, exchange); err != nil {
		return err
	}

	return rt.consumeMessages(queue)
}

func (rt *TransportRabbitMQ) AddConsistentSender(exchange string) error {
	return rt.declareExchange(exchange, "x-consistent-hash")
}

func (rt *TransportRabbitMQ) AddConsistentReceiver(exchange string) error {
	queue := rt.prefix + transport.GenerateUUID()
	if err := rt.declareExchange(exchange, "x-consistent-hash"); err != nil {
		return err
	}

	if err := rt.bindQueue(queue, exchange); err != nil {
		return err
	}

	return rt.consumeMessages(queue)
}

func (rt *TransportRabbitMQ) declareExchange(name, kind string) error {
	if err := rt.channel.ExchangeDeclare(name, kind, false, false, false, false, nil); err != nil {
		rt.logger.Error("ExchangeDeclare error", zap.Error(err))
		return err
	}
	rt.senders[name] = name
	return nil
}

func (rt *TransportRabbitMQ) bindQueue(queue, exchange string) error {
	if _, err := rt.channel.QueueDeclare(queue, false, true, false, false, nil); err != nil {
		return fmt.Errorf("error in declaring the queue: %w", err)
	}

	// 使用整数作为绑定键
	bindingKey := 1 // 这里使用整数 1 作为绑定键
	if err := rt.channel.QueueBind(queue, fmt.Sprintf("%d", bindingKey), exchange, false, nil); err != nil {
		return fmt.Errorf("Queue Bind error: %w", err)
	}
	return nil
}

func (rt *TransportRabbitMQ) consumeMessages(queue string) error {
	deliveries, err := rt.channel.Consume(queue, queue, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Consume error: %w", err)
	}

	go func(msg <-chan amqp.Delivery) {
		for m := range msg {
			if rt.reConnect && string(m.Body) == "" {
				rt.logger.Error("connection closed")
				return
			}
			rt.logger.Info("Read message", zap.String("msg", string(m.Body)))
			rt.msgChan <- m
		}
	}(deliveries)

	return nil
}

// Read 从消息通道中读取一条消息
func (rt *TransportRabbitMQ) Read() (transport.UnificationMessage, error) {
	select {
	case msg := <-rt.msgChan:
		return &RabbitMQMessage{message: msg, topic: msg.Exchange}, nil
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

	if err := rt.publishMessage(exchange, routerKey, p); err != nil {
		rt.logger.Error("Error in Publishing", zap.Error(err))
		return err
	}

	return nil
}

func (rt *TransportRabbitMQ) publishMessage(exchange, routerKey string, p amqp.Publishing) error {
	_, exists := rt.senders[exchange]
	_, existsConsistent := rt.consistentSenders[exchange]
	if exists || existsConsistent {
		err := rt.channel.Publish(exchange, routerKey, false, false, p)
		if err != nil {
			rt.logger.Error("Error in Publishing", zap.Error(err))
			return fmt.Errorf("Error in Publishing: %w", err)
		}
	}
	return nil
}

// Close 关闭 TransportRabbitMQ
func (rt *TransportRabbitMQ) Close() {
	if err := rt.conn.Close(); err != nil {
		rt.logger.Error("Close error", zap.Error(err))
	}
}
