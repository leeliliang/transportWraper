package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/leeliliang/transportWraper/transport"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

type ConfigRabbitMQInfo struct {
	Durable    bool
	AutoDelete bool
	Kind       string
}

type TransportRabbitMQ struct {
	amqpURI       string
	conn          *amqp.Connection
	channel       *amqp.Channel
	senders       map[string]string
	receivers     map[string]<-chan amqp.Delivery
	sendersInfo   map[string]ConfigRabbitMQInfo
	receiversInfo map[string]ConfigRabbitMQInfo
	msgChan       chan amqp.Delivery
	errorChan     chan error
	logger        *zap.Logger
	prefix        string
	reConnect     bool
}

// NewTransportRabbitMQ 创建一个新的 TransportRabbitMQ
func NewTransportRabbitMQ(amqpURI, prefix string, logger *zap.Logger) (*TransportRabbitMQ, error) {
	transportRabbitMQ := &TransportRabbitMQ{
		amqpURI:       amqpURI,
		senders:       make(map[string]string),
		receivers:     make(map[string]<-chan amqp.Delivery),
		sendersInfo:   make(map[string]ConfigRabbitMQInfo),
		receiversInfo: make(map[string]ConfigRabbitMQInfo),
		msgChan:       make(chan amqp.Delivery, 1000),
		errorChan:     make(chan error),
		reConnect:     false,
		logger:        logger,
		prefix:        prefix,
	}

	if err := transportRabbitMQ.Connect(); err != nil {
		logger.Error("Connect error", zap.Error(err))
		return nil, err
	}

	go transportRabbitMQ.monitorConnection()
	go transportRabbitMQ.monitorReconnect()
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
}

func (rt *TransportRabbitMQ) monitorReconnect() {
	if err := <-rt.errorChan; err != nil {
		rt.reConnect = true
		rt.logger.Info("Start reconnect consuming")
		time.Sleep(1 * time.Second)
		if err := rt.Reconnect(); err != nil {
			rt.logger.Error("Reconnect error", zap.Error(err))
			return
		}
	}
	rt.logger.Info("Reconnecting finish")
}

func (rt *TransportRabbitMQ) Reconnect() error {
	rt.logger.Info("Reconnect")
	rt.Close()
	if err := rt.Connect(); err != nil {
		rt.logger.Error("Reconnect error", zap.Error(err))
		return err
	}

	for exchange := range rt.senders {
		mqConfig := rt.sendersInfo[exchange]
		if err := rt.AddSender(exchange, mqConfig.Kind, mqConfig.Durable, mqConfig.AutoDelete); err != nil {
			rt.logger.Error("AddSender error", zap.Error(err))
			return err
		}
	}

	for exchange := range rt.receivers {
		mqConfig := rt.receiversInfo[exchange]
		if err := rt.AddReceiver(exchange, mqConfig.Kind, mqConfig.Durable, mqConfig.AutoDelete); err != nil {
			rt.logger.Error("AddReceiver error", zap.Error(err))
			return err
		}
	}

	return nil
}

func (rt *TransportRabbitMQ) AddSender(exchange, kind string, durable, autoDelete bool) error {
	rt.logger.Info("AddSender", zap.String("exchange", exchange))
	rt.sendersInfo[exchange] = ConfigRabbitMQInfo{
		Durable:    durable,
		AutoDelete: autoDelete,
		Kind:       kind,
	}
	return rt.declareExchange(exchange, kind, durable, autoDelete)
}

func (rt *TransportRabbitMQ) AddReceiver(exchange, kind string, durable, autoDelete bool) error {
	rt.logger.Info("AddReceiver", zap.String("exchange", exchange))
	queue := rt.prefix + transport.GenerateUUID()
	rt.receiversInfo[exchange] = ConfigRabbitMQInfo{
		Durable:    durable,
		AutoDelete: autoDelete,
		Kind:       kind,
	}
	if err := rt.declareExchange(exchange, kind, durable, autoDelete); err != nil {
		return err
	}

	if err := rt.bindQueue(queue, exchange, false, true); err != nil {
		return err
	}

	return rt.consumeMessages(exchange, queue, kind)
}

func (rt *TransportRabbitMQ) declareExchange(name, kind string, durable, autoDelete bool) error {
	if err := rt.channel.ExchangeDeclare(name, kind, durable, autoDelete, false, false, nil); err != nil {
		rt.logger.Error("ExchangeDeclare error", zap.Error(err))
		return err
	}
	rt.senders[name] = name
	return nil
}

func (rt *TransportRabbitMQ) bindQueue(queue, exchange string, durable, autoDelete bool) error {
	if _, err := rt.channel.QueueDeclare(queue, durable, autoDelete, false, false, nil); err != nil {
		return fmt.Errorf("error in declaring the queue: %w", err)
	}

	// 使用整数作为绑定键
	bindingKey := 1 // 这里使用整数 1 作为绑定键
	if err := rt.channel.QueueBind(queue, fmt.Sprintf("%d", bindingKey), exchange, false, nil); err != nil {
		return fmt.Errorf("Queue Bind error: %w", err)
	}
	return nil
}

func (rt *TransportRabbitMQ) consumeMessages(exchange, queue, kind string) error {
	deliveries, err := rt.channel.Consume(queue, queue, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Consume error: %w", err)
	}

	rt.receivers[exchange] = deliveries

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
	if exists {
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
