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
	producer   sarama.AsyncProducer // 改为异步生产者
	logger     *zap.Logger
	brokers    []string
	config     *sarama.Config
	mu         sync.Mutex
	isClosed   bool
	retryDelay time.Duration
	maxRetries int
	wg         sync.WaitGroup // 用于等待异步操作完成
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
	// 对于异步生产者，不需要 Return.Successes
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		logger.Error("Failed to create producer", zap.Error(err))
		return nil, err
	}

	kafkaInstance := &KafkaProducer{
		producer:   producer,
		logger:     logger,
		brokers:    brokers,
		config:     config,
		retryDelay: 5 * time.Second,
		maxRetries: 3,
	}

	// 启动异步处理 goroutine
	go kafkaInstance.handleAsyncResults()

	return kafkaInstance, nil
}

// handleAsyncResults 处理异步发送的结果
func (kp *KafkaProducer) handleAsyncResults() {
	for {
		select {
		case success := <-kp.producer.Successes():
			if success != nil {
				kp.logger.Info("Message delivered to topic",
					zap.String("topic", success.Topic),
					zap.Int32("partition", success.Partition),
					zap.Int64("offset", success.Offset))
			}
		case err := <-kp.producer.Errors():
			if err != nil {
				kp.logger.Error("Failed to send message",
					zap.Error(err.Err),
					zap.String("topic", err.Msg.Topic),
					zap.String("message", string(err.Msg.Value.(sarama.ByteEncoder))))
				// 可以在这里触发重连，但要注意避免无限循环
			}
		case <-time.After(1 * time.Second): // 防止阻塞
			if kp.isClosed {
				return
			}
		}
	}
}

// reconnect 尝试重新连接 Kafka
func (kp *KafkaProducer) reconnect() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.isClosed {
		return nil
	}

	kp.logger.Info("Attempting to reconnect to Kafka", zap.Any("brokers", kp.brokers))

	for i := 0; i < kp.maxRetries; i++ {
		producer, err := sarama.NewAsyncProducer(kp.brokers, kp.config)
		if err == nil {
			// 关闭旧连接并替换
			kp.producer.Close()
			kp.producer = producer
			kp.logger.Info("Successfully reconnected to Kafka")
			// 重启结果处理 goroutine
			go kp.handleAsyncResults()
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

func (kp *KafkaProducer) Write(message []byte, topic, routerKey string, priority int) error {
	kp.mu.Lock()
	if kp.isClosed {
		kp.mu.Unlock()
		return fmt.Errorf("producer is closed")
	}
	kp.mu.Unlock()

	kp.logger.Info("Publish message", zap.String("topic", topic), zap.String("parentRoomID", routerKey))
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(routerKey),
		Value: sarama.ByteEncoder(message),
	}

	kp.wg.Add(1)
	go func() {
		defer kp.wg.Done()

		// 异步发送
		kp.producer.Input() <- msg
	}()

	return nil
}

// Close 关闭 Kafka 生产者
func (kp *KafkaProducer) Close() {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.isClosed {
		return
	}

	kp.isClosed = true
	kp.wg.Wait() // 等待所有消息发送完成
	if err := kp.producer.Close(); err != nil {
		kp.logger.Error("Failed to close producer", zap.Error(err))
	}
}
