package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// 接口路径常量（约定写死，不在配置中出现）
const (
	TokenPath    = "/oauth2/token"                         // 获取 access_token
	OrgPath      = "/api/sync/orgs"                        // 拉取组织机构信息
	UserPath     = "/api/sync/users"                       // 拉取用户信息
	QueryCfgPath = "/v1/rsNpsAccessConfiguration/queryAll" // 从 Java 平台拉取初始配置
)

// NSQ topic 常量（约定写死，不在配置中出现）
const (
	TopicPull      = "rs_pull_nps_data"             // 订阅：接收 rs 模块的拉取指令
	TopicAccessCfg = "rs_nps_access_configuration"  // 订阅：接收 rs 模块的配置变更
	TopicOrg       = "nps_organ_info"              // 发布：组织数据
	TopicUser      = "nps_user_info"               // 发布：用户数据
	PullChannel    = "nps"                          // 订阅 channel
)

// Token 存储常量
const (
	TokenKey   = "nps_token"     // 平台 token 在 Redis 中的键名
	DefaultTTL = 5 * time.Minute // 接口未返回 expires_in 时的默认 TTL（5 分钟）
)

// StartupConfig 启动配置（来自 config.yaml，修改需重启服务）
type StartupConfig struct {
	Redis RedisConfig `yaml:"redis" json:"redis"`
	NSQ   NSQConfig   `yaml:"nsq" json:"nsq"`
	API   APIConfig   `yaml:"api" json:"api"`
	// JavaURL 我们 Java 平台地址，用于启动时拉取初始配置
	JavaURL string `yaml:"java_url" json:"java_url"`
	// NPS 首次启动时作为业务配置的初始值写入 Redis，后续从 Redis 加载
	NPS BizConfig `yaml:"nps" json:"nps"`
	// Platform 对接平台标识，拼接到每条数据的 platform 字段（仅配置文件读取，不走 Redis）
	Platform string `yaml:"platform" json:"platform"`
	// Sync 分页查询配置（区分组织和用户，仅配置文件读取，不走 Redis）
	Sync SyncConfig `yaml:"sync" json:"sync"`
}

// SyncConfig 分页查询配置（区分组织/用户，各自独立）
type SyncConfig struct {
	Orgs  SyncPageConfig `yaml:"orgs" json:"orgs"`
	Users SyncPageConfig `yaml:"users" json:"users"`
	// PushOrganAll true 时组织信息分页拉取、全部拉完后合并一次性转发；false 时按页转发
	PushOrganAll bool `yaml:"push_organInfo_all" json:"pushOrganInfoAll"`
}

// SyncPageConfig 单个接口的分页查询配置
type SyncPageConfig struct {
	// PageSize 每页条数，0 表示默认 50
	PageSize int `yaml:"page_size" json:"pageSize"`
	// Orderby 排序字段，空字符串表示不拼接排序参数
	Orderby string `yaml:"orderby" json:"orderby"`
	// Filters 过滤条件，空字符串表示不拼接过滤参数
	Filters string `yaml:"filters" json:"filters"`
}

// GetPageSize 返回 page_size，未配置时默认 50
func (s *SyncPageConfig) GetPageSize() int {
	if s.PageSize <= 0 {
		return 50
	}
	return s.PageSize
}

// BizConfig 业务配置（存储在 Redis Hash，可通过 HTTP API 热更新）
type BizConfig struct {
	// SSOURL 获取 access_token 的接口前缀，完整地址为 {SSOURL}/oauth2/token
	SSOURL string `yaml:"sso_url" json:"ssoUrl"`
	// IAMAPIURL 组织/用户信息接口前缀，完整地址为 {IAMAPIURL}/api/sync/orgs、{IAMAPIURL}/api/sync/users
	IAMAPIURL         string `yaml:"iamapi_url" json:"iamapiUrl"`
	ClientID          string `yaml:"client_id" json:"clientId"`
	ClientSecret      string `yaml:"client_secret" json:"clientSecret"`
	InsecureSkipVerify bool  `yaml:"insecure_skip_verify" json:"insecureSkipVerify"`
}

// RedisConfig Redis 连接配置（启动配置）
type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	Password string `yaml:"password" json:"password"`
	DB       int    `yaml:"db" json:"db"`
}

// NSQConfig NSQ 连接配置（启动配置）
type NSQConfig struct {
	NSQDAddr string `yaml:"nsqd_addr" json:"nsqd_addr"`
}

// APIConfig HTTP API 服务配置（启动配置）
type APIConfig struct {
	Port int `yaml:"port" json:"port"`
}

// LoadStartup 从指定路径加载启动配置 YAML
func LoadStartup(path string) (*StartupConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	}

	var cfg StartupConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.API.Port == 0 {
		cfg.API.Port = 8080
	}
	return &cfg, nil
}

func (c *StartupConfig) validate() error {
	if c.Redis.Addr == "" {
		return fmt.Errorf("配置缺少 redis.addr")
	}
	if c.NSQ.NSQDAddr == "" {
		return fmt.Errorf("配置缺少 nsq.nsqd_addr")
	}
	if c.Platform == "" {
		return fmt.Errorf("配置缺少 platform")
	}
	return nil
}

// Validate 校验业务配置是否完整
func (b *BizConfig) Validate() error {
	if b.SSOURL == "" {
		return fmt.Errorf("配置缺少 nps.sso_url")
	}
	if b.IAMAPIURL == "" {
		return fmt.Errorf("配置缺少 nps.iamapi_url")
	}
	if b.ClientID == "" {
		return fmt.Errorf("配置缺少 nps.client_id")
	}
	if b.ClientSecret == "" {
		return fmt.Errorf("配置缺少 nps.client_secret")
	}
	return nil
}

// BizConfigToHash 将 BizConfig 转为 Redis Hash 字段
func BizConfigToHash(b *BizConfig) map[string]interface{} {
	return map[string]interface{}{
		"sso_url":              b.SSOURL,
		"iamapi_url":           b.IAMAPIURL,
		"client_id":            b.ClientID,
		"client_secret":        b.ClientSecret,
		"insecure_skip_verify": strconv.FormatBool(b.InsecureSkipVerify),
	}
}

// MapFromHash 将 Redis Hash 字段转为 BizConfig
func MapFromHash(m map[string]string) *BizConfig {
	skipVerify, _ := strconv.ParseBool(m["insecure_skip_verify"])
	return &BizConfig{
		SSOURL:             m["sso_url"],
		IAMAPIURL:          m["iamapi_url"],
		ClientID:           m["client_id"],
		ClientSecret:       m["client_secret"],
		InsecureSkipVerify: skipVerify,
	}
}

// ConnectConfigPayload Java 平台返回的 connectConfig 字段内容（JSON 结构）
type ConnectConfigPayload struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	SSOURL       string `json:"ssoUrl"`
	IAMAPIURL    string `json:"iamapiUrl"`
}

// BizConfigFromConnectConfig 将 Java 平台返回的 connectConfig JSON 字符串转为 BizConfig
func BizConfigFromConnectConfig(jsonStr string) (*BizConfig, error) {
	var p ConnectConfigPayload
	if err := json.Unmarshal([]byte(jsonStr), &p); err != nil {
		return nil, fmt.Errorf("解析 connectConfig 失败: %w", err)
	}
	return &BizConfig{
		SSOURL:        p.SSOURL,
		IAMAPIURL:     p.IAMAPIURL,
		ClientID:      p.ClientID,
		ClientSecret:  p.ClientSecret,
	}, nil
}
