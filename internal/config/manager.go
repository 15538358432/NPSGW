package config

import (
	"fmt"
	"log"
	"sync"
)

// BizConfigStore 业务配置存储接口（由 store 包实现）
type BizConfigStore interface {
	LoadBizConfig() (*BizConfig, error)
	SaveBizConfig(b *BizConfig) error
}

// Manager 线程安全的配置管理器
type Manager struct {
	startup *StartupConfig
	store   BizConfigStore

	mu       sync.RWMutex
	biz      BizConfig
	onUpdate []func(newBiz BizConfig)
}

// NewManager 创建配置管理器
func NewManager(startup *StartupConfig, bizStore BizConfigStore, initialBiz *BizConfig) (*Manager, error) {
	m := &Manager{
		startup: startup,
		store:   bizStore,
	}

	loaded, err := bizStore.LoadBizConfig()
	if err != nil {
		return nil, fmt.Errorf("从 Redis 加载业务配置失败: %w", err)
	}
	if loaded == nil {
		if initialBiz == nil {
			return nil, fmt.Errorf("Redis 中无业务配置，且未提供初始值")
		}
		if err := initialBiz.Validate(); err != nil {
			return nil, fmt.Errorf("初始业务配置校验失败: %w", err)
		}
		if err := bizStore.SaveBizConfig(initialBiz); err != nil {
			return nil, fmt.Errorf("写入初始业务配置到 Redis 失败: %w", err)
		}
		m.biz = *initialBiz
		log.Printf("[config] 已从初始配置写入 Redis Hash: nps_config")
	} else {
		if err := loaded.Validate(); err != nil {
			return nil, fmt.Errorf("Redis 中的业务配置校验失败: %w", err)
		}
		m.biz = *loaded
		log.Printf("[config] 已从 Redis Hash 加载业务配置: ssoUrl=%s", m.biz.SSOURL)
	}
	return m, nil
}

// GetStartup 返回启动配置
func (m *Manager) GetStartup() StartupConfig {
	return *m.startup
}

// GetBiz 返回当前业务配置的副本
func (m *Manager) GetBiz() BizConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.biz
}

// OnUpdate 注册配置更新回调
func (m *Manager) OnUpdate(fn func(newBiz BizConfig)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onUpdate = append(m.onUpdate, fn)
}

// UpdateFields 根据传入的字段映射更新业务配置
// 只更新 map 中存在且值非空的字段，未传入或空值的字段保持不变
// fieldMap 的 key 为驼峰式字段名（与 JSON tag 一致），如 clientId, ssoUrl
func (m *Manager) UpdateFields(fieldMap map[string]string) error {
	m.mu.Lock()
	updated := m.biz

	changedFields := make([]string, 0)

	if v, ok := fieldMap["clientId"]; ok && v != "" {
		updated.ClientID = v
		changedFields = append(changedFields, "clientId")
	}
	if v, ok := fieldMap["clientSecret"]; ok && v != "" {
		updated.ClientSecret = v
		changedFields = append(changedFields, "clientSecret")
	}
	if v, ok := fieldMap["ssoUrl"]; ok && v != "" {
		updated.SSOURL = v
		changedFields = append(changedFields, "ssoUrl")
	}
	if v, ok := fieldMap["iamapiUrl"]; ok && v != "" {
		updated.IAMAPIURL = v
		changedFields = append(changedFields, "iamapiUrl")
	}

	if len(changedFields) == 0 {
		m.mu.Unlock()
		log.Printf("[config] 无需要更新的字段（所有字段为空或未传入）")
		return nil
	}

	log.Printf("[config] 待更新字段: %v", changedFields)

	// 回写 Redis
	if err := m.store.SaveBizConfig(&updated); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("写入业务配置到 Redis 失败: %w", err)
	}

	old := m.biz
	m.biz = updated
	callbacks := make([]func(BizConfig), len(m.onUpdate))
	copy(callbacks, m.onUpdate)
	m.mu.Unlock()

	// 执行回调（已解锁）
	for _, fn := range callbacks {
		fn(updated)
	}
	log.Printf("[config] 业务配置已更新并回写 Redis，变更字段: %v, ssoUrl: %s → %s",
		changedFields, old.SSOURL, updated.SSOURL)
	return nil
}
