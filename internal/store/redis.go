package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"nps/internal/config"
)

// TokenStore token 存储接口
type TokenStore interface {
	Get(key string) (string, error)
	Set(key, value string, ttl time.Duration) error
	Close() error
}

// Redis Redis 存储封装
type Redis struct {
	rdb        *redis.Client
	bizCfgHash string // 存储业务配置的 Redis Hash key
}

// NewRedis 创建 Redis 客户端并验证连通性
func NewRedis(cfg config.RedisConfig, bizCfgHash string) (*Redis, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("连接 Redis %s 失败: %w", cfg.Addr, err)
	}
	if bizCfgHash == "" {
		bizCfgHash = "nps_config"
	}
	return &Redis{rdb: rdb, bizCfgHash: bizCfgHash}, nil
}

// Get 读取 key 对应值，key 不存在时返回空字符串
func (s *Redis) Get(key string) (string, error) {
	v, err := s.rdb.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("读取 Redis key %s 失败: %w", key, err)
	}
	return v, nil
}

// Set 写入 key 并设置过期时间（ttl=0 表示不过期）
func (s *Redis) Set(key, value string, ttl time.Duration) error {
	if err := s.rdb.Set(context.Background(), key, value, ttl).Err(); err != nil {
		return fmt.Errorf("写入 Redis key %s 失败: %w", key, err)
	}
	return nil
}

// LoadBizConfig 从 Redis Hash 加载业务配置
func (s *Redis) LoadBizConfig() (*config.BizConfig, error) {
	m, err := s.rdb.HGetAll(context.Background(), s.bizCfgHash).Result()
	if err != nil {
		return nil, fmt.Errorf("读取 Redis Hash %s 失败: %w", s.bizCfgHash, err)
	}
	if len(m) == 0 {
		return nil, nil // 配置不存在，返回 nil 让调用方初始化
	}
	return config.MapFromHash(m), nil
}

// SaveBizConfig 将业务配置保存到 Redis Hash
func (s *Redis) SaveBizConfig(b *config.BizConfig) error {
	m := config.BizConfigToHash(b)
	if err := s.rdb.HSet(context.Background(), s.bizCfgHash, m).Err(); err != nil {
		return fmt.Errorf("写入 Redis Hash %s 失败: %w", s.bizCfgHash, err)
	}
	return nil
}

// Close 关闭连接
func (s *Redis) Close() error {
	return s.rdb.Close()
}
