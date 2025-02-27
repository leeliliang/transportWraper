package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/leeliliang/transportWraper/transport"
	"go.uber.org/zap"
	"sync"
	"time"
)

// KafkaProducer Kafka 生产者单例
type KafkaProducer struct {
	producer   sarama.SyncProducer
	logger     *zap.Logger
	brokers    []string
	config     *sarama.Config
	mu         sync.Mutex    // 用于并发安全的重连
	isClosed   bool          // 标记连接是否已关闭
	retryDelay time.Duration // 重试间隔
	maxRetries int           // 最大重试次数
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
	config.Producer.Retry.Max = 3                          // 设置默认重试次数
	config.Producer.Retry.Backoff = 100 * time.Millisecond // 重试间隔

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		logger.Error("Failed to create producer", zap.Error(err))
		return nil, err
	}

	kafkaInstance := &KafkaProducer{
		producer:   producer,
		logger:     logger,
		brokers:    brokers,
		config:     config,
		retryDelay: 5 * time.Second, // 默认重试间隔
		maxRetries: 3,               // 默认最大重试次数
	}

	return kafkaInstance, nil
}

// reconnect 尝试重新连接 Kafka
func (kp *KafkaProducer) reconnect() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.isClosed {
		return nil // 如果已关闭，不尝试重连
	}

	kp.logger.Info("Attempting to reconnect to Kafka", zap.Any("brokers", kp.brokers))

	for i := 0; i < kp.maxRetries; i++ {
		producer, err := sarama.NewSyncProducer(kp.brokers, kp.config)
		if err == nil {
			// 成功重连，替换旧的生产者
			kp.producer.Close() // 关闭旧连接
			kp.producer = producer
			kp.logger.Info("Successfully reconnected to Kafka")
			return nil
		}

		kp.logger.Warn("Reconnect attempt failed",
			zap.Int("attempt", i+1),
			zap.Error(err))

		if i < kp.maxRetries-1 {
			time.Sleep(kp.retryDelay)
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts", kp.maxRetries)
}

func (kp *KafkaProducer) Read() (transport.UnificationMessage, error) {
	kafkaMessage := KafkaMessage{message: "", topic: "msg.Exchange"}
	return &kafkaMessage, nil
}

func (kp *KafkaProducer) Write(message []byte, topic, routerKey string) error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	kp.logger.Info("Publish message", zap.String("topic", topic), zap.String("parentRoomID", routerKey))
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(routerKey),
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		kp.logger.Error("Failed to send message",
			zap.Error(err),
			zap.String("topic", topic),
			zap.String("message", string(message)))

		// 尝试重连
		if reconnErr := kp.reconnect(); reconnErr != nil {
			return fmt.Errorf("send failed and reconnect failed: %v", reconnErr)
		}

		// 重连成功后再次尝试发送
		partition, offset, err = kp.producer.SendMessage(msg)
		if err != nil {
			kp.logger.Error("Failed to send message after reconnect",
				zap.Error(err))
			return err
		}
	}

	kp.logger.Info("Message delivered to topic",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))
	return nil
}

// Close 关闭 Kafka 生产者
func (kp *KafkaProducer) Close() {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	kp.isClosed = true
	if err := kp.producer.Close(); err != nil {
		kp.logger.Error("Failed to close producer", zap.Error(err))
	}
}
