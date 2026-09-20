package syncer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"nps/internal/config"
	"nps/internal/nps"
	"nps/internal/nsqx"
)

// pullMsg rs 模块发布到 pull_topic 的指令消息结构
type pullMsg struct {
	Param string `json:"param"` // organ: 组织机构; user: 用户
}

// 分页查询间隔（毫秒），控制对新警通接口的请求频率
const pageInterval = 200 * time.Millisecond

// Syncer 同步编排：接收 pull 指令，按 param 拉取指定数据并透传到 NSQ
type Syncer struct {
	client     *nps.Client
	producer   *nsqx.Producer
	cfgMgr     *config.Manager
	platform   string // 对接平台标识，来自配置文件，不走 Redis
	syncConfig config.SyncConfig
}

// New 创建 Syncer
func New(client *nps.Client, producer *nsqx.Producer, cfgMgr *config.Manager, platform string, syncConfig config.SyncConfig) *Syncer {
	return &Syncer{client: client, producer: producer, cfgMgr: cfgMgr, platform: platform, syncConfig: syncConfig}
}

// HandlePull 处理一条来自 rs 模块的 pull 指令消息
func (s *Syncer) HandlePull(body []byte) error {
	param := s.extractParam(body)
	if param == "" {
		log.Printf("[pull] 忽略无法解析的消息: %s", string(body))
		return nil
	}
	switch param {
	case "organ":
		return s.handleOrgan()
	case "user":
		return s.handleUser()
	default:
		log.Printf("[pull] 忽略未知 param=%s 的消息: %s", param, string(body))
		return nil
	}
}

// extractParam 从消息体中提取 param 值，兼容嵌套 JSON（如 param 字段值为 JSON 字符串）
func (s *Syncer) extractParam(body []byte) string {
	var m pullMsg
	if err := json.Unmarshal(body, &m); err == nil {
		if m.Param == "organ" || m.Param == "user" {
			return m.Param
		}
		// param 可能是嵌套的 JSON 字符串，尝试二次解析
		var nested pullMsg
		if err2 := json.Unmarshal([]byte(m.Param), &nested); err2 == nil {
			if nested.Param == "organ" || nested.Param == "user" {
				return nested.Param
			}
		}
	}
	return ""
}

// normalizeItems 将 API 返回的原始数据解析为列表格式，并为每项添加 optType、platform、转换时间格式
// 兼容 API 返回 {data: [...]} 包装、单个对象、或纯数组三种情况
func (s *Syncer) normalizeItems(raw json.RawMessage, optType string) ([]map[string]interface{}, error) {
	var items []map[string]interface{}

	// 先尝试解析为列表
	if err := json.Unmarshal(raw, &items); err == nil {
		// 纯数组格式，直接使用
	} else {
		// 尝试解析为对象，看是否有 data 字段
		var wrapper map[string]interface{}
		if err2 := json.Unmarshal(raw, &wrapper); err2 == nil {
			if rawData, ok := wrapper["data"]; ok {
				// 有 data 字段，尝试提取数组
				if dataBytes, err3 := json.Marshal(rawData); err3 == nil {
					if err4 := json.Unmarshal(dataBytes, &items); err4 == nil {
						// data 是数组，使用它
					} else {
						// data 是单个对象，包装成列表
						var obj map[string]interface{}
						if err5 := json.Unmarshal(dataBytes, &obj); err5 == nil {
							items = []map[string]interface{}{obj}
						} else {
							return nil, fmt.Errorf("解析 data 字段失败: %v", err5)
						}
					}
				}
			} else {
				// 无 data 字段，可能是单个对象
				items = []map[string]interface{}{wrapper}
			}
		} else {
			return nil, fmt.Errorf("解析接口数据失败: %v", err2)
		}
	}

	if len(items) == 0 {
		log.Printf("[normalizeData] 数据为空，跳过处理")
		return items, nil
	}

	log.Printf("[normalizeData] 提取到 %d 条数据，开始添加 optType/platform 并转换时间格式", len(items))

	s.decorateItems(items, optType)
	return items, nil
}

// decorateItems 为每项添加 optType、platform，并转换时间字段格式（全量/变更事件共用）
func (s *Syncer) decorateItems(items []map[string]interface{}, optType string) {
	for i := range items {
		items[i]["optType"] = optType
		if s.platform != "" {
			items[i]["platform"] = s.platform
		}
		s.convertTimeField(items[i], "createdAt")
		s.convertTimeField(items[i], "updatedAt")

		// 只打印第一条的转换结果
		if i == 0 {
			log.Printf("[normalizeData] 第1条 createdAt=%v, updatedAt=%v",
				items[i]["createdAt"], items[i]["updatedAt"])
		}
	}
}

// normalizeData 将归一化后的列表序列化为 JSON 并返回条数（按页发布场景使用）
func (s *Syncer) normalizeData(raw json.RawMessage) (json.RawMessage, int, error) {
	items, err := s.normalizeItems(raw, "all")
	if err != nil {
		return nil, 0, err
	}
	result, err := json.Marshal(items)
	if err != nil {
		return nil, 0, fmt.Errorf("序列化数据失败: %v", err)
	}
	return result, len(items), nil
}

// convertTimeField 将指定字段的时间从 ISO 8601 格式转为 "2006-01-02 15:04:05"
// 支持格式: 2025-03-27T15:31:42.223+08:00 或 2025-03-27T15:31:42+08:00
func (s *Syncer) convertTimeField(item map[string]interface{}, field string) {
	val, ok := item[field]
	if !ok {
		return
	}
	str, ok := val.(string)
	if !ok || str == "" {
		return
	}

	// 尝试带毫秒格式
	if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
		item[field] = t.Format("2006-01-02 15:04:05")
		return
	}
	// 尝试不带毫秒格式
	if t, err := time.Parse(time.RFC3339, str); err == nil {
		item[field] = t.Format("2006-01-02 15:04:05")
		return
	}
	// 尝试无时区带毫秒格式（RabbitMQ 变更事件样例: 2025-05-16T11:38:40.443）
	if t, err := time.Parse("2006-01-02T15:04:05.000", str); err == nil {
		item[field] = t.Format("2006-01-02 15:04:05")
		return
	}
	// 尝试无时区不带毫秒格式
	if t, err := time.Parse("2006-01-02T15:04:05", str); err == nil {
		item[field] = t.Format("2006-01-02 15:04:05")
		return
	}

	log.Printf("[time] 时间格式转换失败: field=%s, value=%s", field, str)
}

// SyncAll 启动时主动拉取一次全量组织和用户数据
func (s *Syncer) SyncAll() {
	log.Printf("[startup] 开始主动拉取全量组织/用户数据...")
	if err := s.handleOrgan(); err != nil {
		log.Printf("[startup] 拉取组织数据失败，后续可等待拉取指令重试: %v", err)
	}
	if err := s.handleUser(); err != nil {
		log.Printf("[startup] 拉取用户数据失败，后续可等待拉取指令重试: %v", err)
	}
	log.Printf("[startup] 主动全量拉取结束")
}

// handleOrgan 分页拉取组织机构数据并透传
// push_organInfo_all=true 时：分页拉取但先累积，全部拉完后合并为一条消息一次性转发；否则按页转发
func (s *Syncer) handleOrgan() error {
	sp := s.syncConfig.Orgs
	size := sp.GetPageSize()
	log.Printf("[pull] param=organ，开始分页拉取组织机构信息: page_size=%d, orderby=%s, filters=%q, push_organInfo_all=%v",
		size, sp.Orderby, sp.Filters, s.syncConfig.PushOrganAll)

	page := 1
	total := 0
	all := make([]map[string]interface{}, 0)
	for {
		raw, err := s.client.FetchOrg(page, size, sp.Orderby, sp.Filters)
		if err != nil {
			return fmt.Errorf("拉取组织第%d页失败: %w", page, err)
		}

		items, err := s.normalizeItems(raw, "all")
		if err != nil {
			return fmt.Errorf("转换组织数据格式失败(第%d页): %w", page, err)
		}
		cnt := len(items)
		log.Printf("[pull] param=organ 第%d页: %d 条，累计 %d 条", page, cnt, total+cnt)

		if cnt == 0 {
			log.Printf("[pull] param=organ 第%d页返回空，结束分页", page)
			break
		}

		if s.syncConfig.PushOrganAll {
			// 累积模式：先不发布，等全部拉完后一次性转发
			all = append(all, items...)
		} else {
			normalized, err := json.Marshal(items)
			if err != nil {
				return fmt.Errorf("序列化组织数据失败(第%d页): %w", page, err)
			}
			if err := s.publishRaw(config.TopicOrg, normalized, "org", cnt); err != nil {
				return fmt.Errorf("透传组织数据失败(第%d页): %w", page, err)
			}
		}

		total += cnt

		// 终止条件：最后一页
		if cnt < size {
			log.Printf("[pull] param=organ 第%d页返回%d条 < page_size=%d，分页完成", page, cnt, size)
			break
		}

		page++
		time.Sleep(pageInterval)
	}

	if s.syncConfig.PushOrganAll {
		normalized, err := json.Marshal(all)
		if err != nil {
			return fmt.Errorf("序列化组织数据失败: %w", err)
		}
		if err := s.publishRaw(config.TopicOrg, normalized, "org", total); err != nil {
			return fmt.Errorf("透传组织数据失败: %w", err)
		}
	}

	log.Printf("[pull] param=organ 分页拉取完成，共 %d 页，total=%d 条", page, total)
	return nil
}

// handleUser 分页拉取用户信息并透传
func (s *Syncer) handleUser() error {
	sp := s.syncConfig.Users
	size := sp.GetPageSize()
	log.Printf("[pull] param=user，开始分页拉取用户信息: page_size=%d, orderby=%s, filters=%q",
		size, sp.Orderby, sp.Filters)

	page := 1
	total := 0
	for {
		raw, err := s.client.FetchUser(page, size, sp.Orderby, sp.Filters)
		if err != nil {
			return fmt.Errorf("拉取用户第%d页失败: %w", page, err)
		}

		// 归一化 + 发布（复用条数，避免二次解析）
		normalized, cnt, err := s.normalizeData(raw)
		if err != nil {
			return fmt.Errorf("转换用户数据格式失败(第%d页): %w", page, err)
		}
		log.Printf("[pull] param=user 第%d页: %d 条，累计 %d 条", page, cnt, total+cnt)

		if cnt == 0 {
			log.Printf("[pull] param=user 第%d页返回空，结束分页", page)
			break
		}

		if err := s.publishRaw(config.TopicUser, normalized, "user", cnt); err != nil {
			return fmt.Errorf("透传用户数据失败(第%d页): %w", page, err)
		}

		total += cnt

		if cnt < size {
			log.Printf("[pull] param=user 第%d页返回%d条 < page_size=%d，分页完成", page, cnt, size)
			break
		}

		page++
		time.Sleep(pageInterval)
	}

	log.Printf("[pull] param=user 分页拉取完成，共 %d 页，total=%d 条", page, total)
	return nil
}

// accessCfgMsg rs 模块发布到配置变更 topic 的消息结构
type accessCfgMsg struct {
	Name          string `json:"name"`          // 对接平台名称
	ConnectConfig string `json:"connectConfig"` // 连接配置 JSON 字符串
	Type          string `json:"type"`          // 操作类型: del/add/update/getAll
}

// HandleAccessConfig 处理来自 rs 模块的配置变更消息
// 解析 connectConfig，将不为空的字段更新到 Redis
func (s *Syncer) HandleAccessConfig(body []byte) error {
	var msg accessCfgMsg
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("[cfg] 解析配置变更消息失败: %v, 原始消息: %s", err, string(body))
		return nil
	}

	log.Printf("[cfg] 收到配置变更: name=%s, type=%s, connectConfig=%s", msg.Name, msg.Type, msg.ConnectConfig)

	if strings.TrimSpace(msg.ConnectConfig) == "" {
		log.Printf("[cfg] connectConfig 为空，跳过")
		return nil
	}

	biz, err := config.BizConfigFromConnectConfig(msg.ConnectConfig)
	if err != nil {
		log.Printf("[cfg] 解析 connectConfig 失败: %v", err)
		return nil
	}

	// 只更新不为空的字段
	fieldMap := make(map[string]string)
	if strings.TrimSpace(biz.ClientID) != "" {
		fieldMap["clientId"] = biz.ClientID
	}
	if strings.TrimSpace(biz.ClientSecret) != "" {
		fieldMap["clientSecret"] = biz.ClientSecret
	}
	if strings.TrimSpace(biz.SSOURL) != "" {
		fieldMap["ssoUrl"] = biz.SSOURL
	}
	if strings.TrimSpace(biz.IAMAPIURL) != "" {
		fieldMap["iamapiUrl"] = biz.IAMAPIURL
	}

	if len(fieldMap) == 0 {
		log.Printf("[cfg] 所有字段均为空，跳过更新")
		return nil
	}

	log.Printf("[cfg] 待更新字段: %v", fieldMap)

	if err := s.cfgMgr.UpdateFields(fieldMap); err != nil {
		log.Printf("[cfg] 配置更新失败: %v", err)
		return fmt.Errorf("配置更新失败: %w", err)
	}

	log.Printf("[cfg] 配置更新成功")
	return nil
}

// eventMsg 新警通平台通过 RabbitMQ 推送的变更事件结构（CloudEvents 格式）
type eventMsg struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// eventTypeMap 变更事件 type → (optType, NSQ topic)
var eventTypeMap = map[string]struct {
	optType string
	topic   string
}{
	"iam.org.created":  {"add", config.TopicOrg},
	"iam.org.updated":  {"update", config.TopicOrg},
	"iam.org.deleted":  {"del", config.TopicOrg},
	"iam.user.created": {"add", config.TopicUser},
	"iam.user.updated": {"update", config.TopicUser},
	"iam.user.deleted": {"del", config.TopicUser},
}

// HandleEvent 处理 RabbitMQ 变更事件：根据 type 映射 optType，将 data 包装为 JSONArray 后透传到 NSQ
// 解析失败/未知 type/数据为空 均记日志后按消费成功处理（不重投），仅 NSQ 发布失败才重投
func (s *Syncer) HandleEvent(body []byte) error {
	var msg eventMsg
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("[event] 解析变更事件失败，丢弃: %v, 原始消息: %s", err, string(body))
		return nil
	}
	mapping, ok := eventTypeMap[msg.Type]
	if !ok {
		log.Printf("[event] 忽略未知 type=%s 的事件: id=%s", msg.Type, msg.ID)
		return nil
	}

	items, err := s.normalizeItems(msg.Data, mapping.optType)
	if err != nil {
		log.Printf("[event] 转换事件数据失败，丢弃: id=%s, type=%s, err=%v", msg.ID, msg.Type, err)
		return nil
	}
	if len(items) == 0 {
		log.Printf("[event] 事件数据为空，跳过: id=%s, type=%s", msg.ID, msg.Type)
		return nil
	}

	normalized, err := json.Marshal(items)
	if err != nil {
		log.Printf("[event] 序列化事件数据失败，丢弃: id=%s, err=%v", msg.ID, err)
		return nil
	}
	if err := s.publishRaw(mapping.topic, normalized, mapping.optType, len(items)); err != nil {
		return fmt.Errorf("透传变更事件失败: %w", err)
	}
	log.Printf("[event] 变更事件已转发: id=%s, type=%s, optType=%s, topic=%s", msg.ID, msg.Type, mapping.optType, mapping.topic)
	return nil
}

// prettyJSON 将 JSON 格式化为缩进形式用于日志打印，失败时返回原始字符串
func prettyJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "    "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// publishRaw 向指定 topic 发布原始 JSON 数据（已归一化为列表格式）
// count 由 normalizeData 直接传入，避免在此处二次反序列化
func (s *Syncer) publishRaw(topic string, data json.RawMessage, dataType string, count int) error {
	if s.producer == nil || topic == "" {
		log.Printf("[NSQ] 跳过 %s 透传（producer 未启用或 topic 为空）", dataType)
		return nil
	}
	payloadLen := len(data)
	log.Printf("[NSQ] 开始发布: topic=%s, data_type=%s, 数据条数=%d, 消息大小=%d bytes", topic, dataType, count, payloadLen)
	if err := s.producer.Publish(topic, data); err != nil {
		log.Printf("[NSQ] 发布失败: topic=%s, data_type=%s, 错误=%v", topic, dataType, err)
		return fmt.Errorf("发布 %s 消息失败: %w", dataType, err)
	}
	log.Printf("[NSQ] 发布完成: topic=%s, data_type=%s, 数据条数=%d, 消息大小=%d bytes", topic, dataType, count, payloadLen)
	return nil
}
