package nsqx

import (
	"fmt"
	"log"

	"github.com/nsqio/go-nsq"
)

// Producer 封装 go-nsq producer，负责向内部资源管理模块透传消息
type Producer struct {
	p *nsq.Producer
}

// NewProducer 连接指定 nsqd 地址创建 producer
func NewProducer(addr string) (*Producer, error) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 NSQ producer(%s) 失败: %w", addr, err)
	}
	p.SetLogger(log.Default(), nsq.LogLevelWarning)
	return &Producer{p: p}, nil
}

// Publish 向指定 topic 发布一条消息
func (p *Producer) Publish(topic string, body []byte) error {
	if err := p.p.Publish(topic, body); err != nil {
		return fmt.Errorf("发布消息到 topic %s 失败: %w", topic, err)
	}
	return nil
}

// Stop 优雅停止 producer
func (p *Producer) Stop() {
	p.p.Stop()
}
