package mqx

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler 消息处理函数，返回 error 会让消息重新入队
type Handler func(body []byte) error

// Consumer 封装 RabbitMQ consumer：消费指定队列并交由 Handler 处理，连接断开自动重连
type Consumer struct {
	url     string
	queue   string
	handler Handler
	stopCh  chan struct{}
}

// NewConsumer 创建 consumer 并启动后台消费循环
// url 为 RabbitMQ 连接地址（amqp://user:pass@host:port/vhost），queue 为队列名
func NewConsumer(url, queue string, handler Handler) *Consumer {
	c := &Consumer{url: url, queue: queue, handler: handler, stopCh: make(chan struct{})}
	go c.run()
	return c
}

// run 消费主循环：连接断开后按退避间隔自动重连，直到 Stop
func (c *Consumer) run() {
	backoff := 2 * time.Second
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}
		if err := c.consumeOnce(); err != nil {
			log.Printf("[RabbitMQ] 消费异常: %v，%v 后重连", err, backoff)
		}
		select {
		case <-c.stopCh:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// consumeOnce 建立一次连接并消费，返回时代表连接已断开或已停止
func (c *Consumer) consumeOnce() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 RabbitMQ channel 失败: %w", err)
	}
	defer ch.Close()

	// 被动声明：只校验队列存在，不创建（队列由平台侧创建并绑定）
	q, err := ch.QueueDeclarePassive(c.queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("队列 %s 不存在或声明失败（请确认平台已创建该队列）: %w", c.queue, err)
	}
	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("设置 Qos 失败: %w", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("开始消费队列 %s 失败: %w", c.queue, err)
	}
	log.Printf("[RabbitMQ] 开始消费: queue=%s", c.queue)

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-c.stopCh:
			return nil
		case amqErr := <-closeCh:
			return fmt.Errorf("RabbitMQ 连接断开: %v", amqErr)
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("RabbitMQ 消息通道已关闭")
			}
			if err := c.handler(d.Body); err != nil {
				log.Printf("[RabbitMQ] 消息处理失败，重新入队: %v", err)
				_ = d.Nack(false, true)
			} else {
				_ = d.Ack(false)
			}
		}
	}
}

// Stop 停止消费
func (c *Consumer) Stop() {
	close(c.stopCh)
}
