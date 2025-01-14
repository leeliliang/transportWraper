package kafka

import (
	"github.com/IBM/sarama"
	"github.com/leeliliang/transportWraper/transport"
	"go.uber.org/zap"
)

// KafkaProducer Kafka 生产者单例
type KafkaProducer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
}

// GetKafkaProducer 获取 Kafka 生产者单例
func GetKafkaProducer(brokers []string, partition int, logger *zap.Logger) (*KafkaProducer, error) {
	logger.Info("GetKafkaProducer", zap.Any("brokers", brokers), zap.Int("partition", partition))

	config := sarama.NewConfig()
	if partition != 0 {
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return NewCustomPartitioner(partition, logger)
		}
	}
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		logger.Error("Failed to create producer", zap.Error(err))
		return nil, err
	}
	kafkaInstance := &KafkaProducer{
		producer: producer,
		logger:   logger,
	}
	return kafkaInstance, nil
}

func (kp *KafkaProducer) Read() (transport.UnificationMessage, error) {
	kafkaMessage := KafkaMessage{message: "", topic: "msg.Exchange"}
	return &kafkaMessage, nil
}

func (kp *KafkaProducer) Write(message []byte, topic, routerKey string) error {
	// 创建 Kafka 消息
	kp.logger.Info("Publish message", zap.String("topic", topic), zap.String("parentRoomID", routerKey))
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(routerKey),
		Value: sarama.ByteEncoder(message),
	}

	// 发送消息
	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		kp.logger.Error("Failed to send message", zap.Error(err), zap.String("topic", topic), zap.String("message", string(message)))
		return err
	}
	kp.logger.Info("Message delivered to topic", zap.String("topic", topic), zap.Int32("partition", partition), zap.Int64("offset", offset))
	return nil
}

// Close 关闭 Kafka 生产者
func (kp *KafkaProducer) Close() {
	if err := kp.producer.Close(); err != nil {
		kp.logger.Error("Failed to close producer", zap.Error(err))
	}
}
