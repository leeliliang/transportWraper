package main

import (
	"fmt"
	"github.com/leeliliang/transportWraper/transport"
	"github.com/leeliliang/transportWraper/transport/kafka"
	"github.com/leeliliang/transportWraper/transport/rabbitmq"
	"go.uber.org/zap"
	"strconv"
	"time"
)

func main() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // 设置日志级别为 Debug

	// 使用配置创建 logger
	logger, _ := config.Build()
	defer logger.Sync()
	var unifiedTransport *transport.UnifiedTransport
	rabbitMQUri := "amqp://" + "" + ":" + "" + "@" + "" + ":" + strconv.Itoa(5672) + "/" + "" + "?heartbeat=3&connection_timeout=180000"

	rabbitmqInstance, err := rabbitmq.NewTransportRabbitMQ(rabbitMQUri, logger)
	if err != nil {
		logger.Error("Failed to create RabbitMQ transport", zap.Error(err))
		return
	}
	broke := []string{""}
	kafkaInstance, err := kafka.GetKafkaProducer(broke, 2, logger)
	if err != nil {
		logger.Error("Failed to create Kafka transport", zap.Error(err))
		return
	}
	send01Config := &rabbitmq.ConfigRabbitMQInfo{
		ID:                 "1",
		Exchange:           "test01",
		Kind:               "fanout",
		Queue:              "test01",
		BindingKey:         "",
		ExchangeDurable:    true,
		ExchangeAutoDelete: true,
		QueueDurable:       true,
		QueueAutoDelete:    true,
		Priority:           0,
	}
	err = rabbitmqInstance.AddSender(send01Config)
	if err != nil {
		logger.Error("Failed to add sender", zap.Error(err))
		return
	}
	send02Config := &rabbitmq.ConfigRabbitMQInfo{
		ID:                 "2",
		Exchange:           "test02",
		Kind:               "fanout",
		Queue:              "test02",
		BindingKey:         "",
		ExchangeDurable:    true,
		ExchangeAutoDelete: true,
		QueueDurable:       true,
		QueueAutoDelete:    true,
		Priority:           0,
	}
	err = rabbitmqInstance.AddSender(send02Config)
	if err != nil {
		logger.Error("Failed to add sender", zap.Error(err))
		return
	}
	receiver01Config := &rabbitmq.ConfigRabbitMQInfo{
		ID:                 "2",
		Exchange:           "test02",
		Kind:               "fanout",
		Queue:              "test02",
		BindingKey:         "",
		ExchangeDurable:    true,
		ExchangeAutoDelete: true,
		QueueDurable:       true,
		QueueAutoDelete:    true,
		Priority:           0,
	}
	err = rabbitmqInstance.AddReceiver(receiver01Config)
	if err != nil {
		logger.Error("Failed to add receiver", zap.Error(err))
		return
	}
	receiver02Config := &rabbitmq.ConfigRabbitMQInfo{
		ID:                 "1",
		Exchange:           "test01",
		Kind:               "fanout",
		Queue:              "test01",
		BindingKey:         "",
		ExchangeDurable:    true,
		ExchangeAutoDelete: true,
		QueueDurable:       true,
		QueueAutoDelete:    true,
		Priority:           0,
	}
	err = rabbitmqInstance.AddReceiver(receiver02Config)
	if err != nil {
		logger.Error("Failed to add receiver", zap.Error(err))
		return
	}

	unifiedTransport = transport.NewUnifiedTransport()
	unifiedTransport.AddSender("test01", rabbitmqInstance)
	unifiedTransport.AddSender("test02", rabbitmqInstance)
	unifiedTransport.AddSender("testconsistent01", rabbitmqInstance)
	unifiedTransport.AddSender("testconsistent02", rabbitmqInstance)
	unifiedTransport.AddSender("", kafkaInstance)
	unifiedTransport.AddReceiver("mq", rabbitmqInstance)

	go func() {
		// Create a ticker that fires every second
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop() // Ensure ticker is stopped when goroutine exits

		for {
			// Wait for the next tick
			<-ticker.C

			// Send messages
			unifiedTransport.Write([]byte("test01"), "test01", "test01", 0)
			unifiedTransport.Write([]byte("test02"), "test02", "test02", 0)
			unifiedTransport.Write([]byte("testconsistent01"), "testconsistent01", "consistent01", 0)
			unifiedTransport.Write([]byte("testconsistent02"), "testconsistent02", "consistent02", 0)
			unifiedTransport.Write([]byte(""), "", "123", 0)

			// Optional: Add logging to confirm messages are sent
			fmt.Println("Messages sent at:", time.Now())
		}
	}()

	for {
		msg, readErr := unifiedTransport.Read()
		if readErr != nil {
			logger.Error("Error reading message", zap.Error(readErr))
			continue
		}
		logger.Info("Received a message", zap.String("msg", string(msg.GetBody())), zap.String("exchange", msg.GetTopic()))
	}
	defer unifiedTransport.Close()
	select {}
}
