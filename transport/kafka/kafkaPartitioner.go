package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/leeliliang/transportWraper/transport"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

// CustomPartitioner 实现了 Partitioner 接口
type CustomPartitioner struct {
	ch            *transport.ConsistentHash
	logger        *zap.Logger
	DataPartition int
}

// NewCustomPartitioner 创建一个新的自定义分区器
func NewCustomPartitioner(partitionCount int, logger *zap.Logger) *CustomPartitioner {
	// 创建一致性哈希实例
	consumers := make([]string, partitionCount)
	for i := 0; i < partitionCount; i++ {
		consumers[i] = "liveConsumer" + strconv.Itoa(i) // 假设分区名称为 "0", "1", ...
	}
	ch := transport.New(partitionCount, transport.MurmurHash) // 3 是虚拟节点数
	ch.Add(consumers)
	return &CustomPartitioner{
		ch:            ch,
		logger:        logger,
		DataPartition: partitionCount,
	}
}

// Partition 实现了 Partitioner 接口的方法
func (p *CustomPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if msg.Key == nil {
		// 如果没有键，则随机选择一个分区
		return 0, nil
	}
	key, err := msg.Key.Encode()
	if err != nil || len(key) == 0 {
		return 0, err // 键编码失败，返回错误
	}
	// 使用一致性哈希选择分区
	p.logger.Debug("key", zap.String("key", string(key)))
	consumer := p.ch.Get(string(key))
	if consumer == "" {
		return 0, fmt.Errorf("no consumer found for key: %s", string(key)) // 无法找到消费者，返回错误
	}
	p.logger.Debug("consumer", zap.String("consumer", consumer))
	prefix := "liveConsumer"
	// 去掉前缀
	result := strings.TrimPrefix(consumer, prefix)

	partition, err := strconv.Atoi(result) // 转换为整数分区
	if err != nil {
		return 0, err
	}
	p.logger.Debug("partition", zap.Int("partition", partition))
	partition = partition % p.DataPartition
	p.logger.Debug("partition", zap.Int("partition", partition), zap.String("key", string(key)))
	return int32(partition), nil
}

func (p *CustomPartitioner) RequiresConsistency() bool {
	return false
}
