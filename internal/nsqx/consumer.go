package nsqx

import (
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

// Handler 消息处理函数，返回 error 会让 NSQ 重投
type Handler func(body []byte) error

// Consumer 封装 go-nsq consumer：订阅指定 topic/channel 并交由 Handler 处理
type Consumer struct {
	c  *nsq.Consumer
}

// NewConsumer 创建 consumer；addr 为 nsqd 的 TCP 地址（如 127.0.0.1:4150）
// topic、channel 由调用方传入；channel 为空时使用默认值
func NewConsumer(addr, topic, channel string, handler Handler) (*Consumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("NSQ pull_topic 不能为空")
	}
	if channel == "" {
		channel = "nps"
	}
	cfg := nsq.NewConfig()
	// 单条消息处理超时：120 秒（拉取接口可能慢）
	cfg.MsgTimeout = 120 * time.Second
	// 消费失败最大重试次数
	cfg.MaxAttempts = 3

	nc, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 NSQ consumer 失败: %w", err)
	}
	nc.SetLogger(log.Default(), nsq.LogLevelWarning)
	nc.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		if m == nil {
			return nil
		}
		// 手动 DisableAutoResponse，按 handler 返回值决定是否 Finish/Requeue
		m.DisableAutoResponse()
		if err := handler(m.Body); err != nil {
			log.Printf("[NSQ] 处理消息失败，稍后重试: topic=%s, body=%s, err=%v", topic, string(m.Body), err)
			m.RequeueWithoutBackoff(5 * time.Second)
			return err
		}
		m.Finish()
		return nil
	}))

	// 直连 nsqd
	if err := nc.ConnectToNSQD(addr); err != nil {
		return nil, fmt.Errorf("NSQ consumer 连接 nsqd(%s) 失败: %w", addr, err)
	}
	log.Printf("[NSQ] 订阅成功: topic=%s, channel=%s", topic, channel)
	return &Consumer{c: nc}, nil
}

// Stop 优雅停止 consumer
func (c *Consumer) Stop() {
	c.c.Stop()
}
