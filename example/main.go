package main

import (
	"github.com/leeliliang/transportWraper/transport"
	"github.com/leeliliang/transportWraper/transport/kafka"
	"github.com/leeliliang/transportWraper/transport/rabbitmq"
	"go.uber.org/zap"
	"strconv"
)

func main() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // 设置日志级别为 Debug

	// 使用配置创建 logger
	logger, _ := config.Build()
	defer logger.Sync()
	rabbitMQUri := "amqp://" + "" + ":" + "" + "@" + "" + ":" + strconv.Itoa(5672) + "/" + "" + "?heartbeat=3&connection_timeout=180000"
	prefix := "test"
	var unifiedTransport *transport.UnifiedTransport
	rabbitmqInstance, err := rabbitmq.NewTransportRabbitMQ(rabbitMQUri, prefix, logger)
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

	err = rabbitmqInstance.AddSender("test01", "facout", true, true)
	if err != nil {
		logger.Error("Failed to add sender", zap.Error(err))
		return
	}

	err = rabbitmqInstance.AddSender("test02", "facout", true, true)
	if err != nil {
		logger.Error("Failed to add sender", zap.Error(err))
		return
	}

	err = rabbitmqInstance.AddReceiver("test01", "facout", "", true, true)
	if err != nil {
		logger.Error("Failed to add receiver", zap.Error(err))
		return
	}

	err = rabbitmqInstance.AddReceiver("test02", "facout", "", true, true)
	if err != nil {
		logger.Error("Failed to add receiver", zap.Error(err))
		return
	}

	unifiedTransport = transport.NewUnifiedTransport()
	unifiedTransport.AddSender("test01", rabbitmqInstance)
	unifiedTransport.AddSender("test02", rabbitmqInstance)
	unifiedTransport.AddSender("testconsistent01", rabbitmqInstance)
	unifiedTransport.AddSender("testconsistent02", rabbitmqInstance)
	unifiedTransport.AddSender("dev_bigdata_live_room_interaction_record", kafkaInstance)

	unifiedTransport.AddReceiver("mq", rabbitmqInstance)

	go func() {
		unifiedTransport.Write([]byte("test01"), "test01", "test01")
		unifiedTransport.Write([]byte("test02"), "test02", "test02")
		unifiedTransport.Write([]byte("testconsistent01"), "testconsistent01", "consistent01")
		unifiedTransport.Write([]byte("testconsistent02"), "testconsistent02", "consistent02")
		unifiedTransport.Write([]byte("dev_bigdata_live_room_interaction_record"), "dev_bigdata_live_room_interaction_record", "")
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
