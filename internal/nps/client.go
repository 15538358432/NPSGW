package nps

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"nps/internal/config"
)

// TokenResponse 新警通平台 getToken 接口返回结构
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

// TokenStore token 的外部存储（Redis），支持 TTL
type TokenStore interface {
	Get(key string) (string, error)
	Set(key, value string, ttl time.Duration) error
}

// Client 新警通平台 HTTP 客户端
type Client struct {
	cfgMgr *config.Manager // 配置管理器，运行时可热更新
	store  TokenStore

	httpMu     sync.RWMutex
	http       *http.Client
	skipVerify bool // 上次创建 http.Client 时的 InsecureSkipVerify 值

	cached      string    // 内存缓存的 access_token
	cachedUntil time.Time // 内存缓存的过期时间
}

// NewClient 创建客户端
func NewClient(cfgMgr *config.Manager, store TokenStore) *Client {
	biz := cfgMgr.GetBiz()
	c := &Client{
		cfgMgr:     cfgMgr,
		store:      store,
		skipVerify: biz.InsecureSkipVerify,
		http:       newHTTPClient(biz.InsecureSkipVerify),
	}
	// 注册配置更新回调：InsecureSkipVerify 变更时重建 HTTP Client，并清空 token 缓存
	cfgMgr.OnUpdate(func(newBiz config.BizConfig) {
		if newBiz.InsecureSkipVerify != c.skipVerify {
			c.httpMu.Lock()
			c.http = newHTTPClient(newBiz.InsecureSkipVerify)
			c.skipVerify = newBiz.InsecureSkipVerify
			c.httpMu.Unlock()
			log.Printf("[client] InsecureSkipVerify 已变更为 %v，HTTP Client 已重建", newBiz.InsecureSkipVerify)
		}
		c.cached = ""
		c.cachedUntil = time.Time{}
		log.Printf("[client] 业务配置已更新，token 缓存已清空")
	})
	return c
}

func newHTTPClient(skipVerify bool) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: skipVerify},
		},
	}
}

func (c *Client) getHTTPClient() *http.Client {
	c.httpMu.RLock()
	defer c.httpMu.RUnlock()
	return c.http
}

// EnsureToken 确保 access_token 可用：内存未过期 -> Redis -> 调接口获取并写回（带 TTL）
func (c *Client) EnsureToken() (string, error) {
	if c.cached != "" && time.Now().Before(c.cachedUntil) {
		return c.cached, nil
	}
	if v, err := c.store.Get(config.TokenKey); err != nil {
		return "", err
	} else if v != "" {
		c.cached = v
		c.cachedUntil = time.Now().Add(config.DefaultTTL).Add(-30 * time.Second)
		return v, nil
	}
	log.Printf("[token] access_token 已过期或不存在，正在从新警通平台重新获取...")
	t, ttl, err := c.fetchToken()
	if err != nil {
		return "", err
	}
	if ttl <= 0 {
		ttl = config.DefaultTTL
	}
	if err := c.store.Set(config.TokenKey, t, ttl); err != nil {
		return "", err
	}
	c.cached = t
	c.cachedUntil = time.Now().Add(ttl).Add(-30 * time.Second)
	return t, nil
}

// InvalidateToken 清空缓存的 access_token，下次 EnsureToken 会重新获取
func (c *Client) InvalidateToken() {
	c.cached = ""
	c.cachedUntil = time.Time{}
}

// fetchToken 调用 getToken 接口
// 请求方式：POST {sso_url}/oauth2/token
// 请求头：Authorization: Basic Base64(client_id:client_secret)
// 请求体：grant_type=client_credentials（form 表单）
func (c *Client) fetchToken() (string, time.Duration, error) {
	biz := c.cfgMgr.GetBiz()
	url := biz.SSOURL + config.TokenPath
	log.Printf("[getToken] 开始请求: POST %s", url)

	// 构造 Basic Auth：Base64(client_id:client_secret)
	credentials := biz.ClientID + ":" + biz.ClientSecret
	basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(credentials))
	log.Printf("[getToken] Basic Auth 调试: client_id=%q, client_secret=%q, credentials=%q, Authorization=%q",
		biz.ClientID, biz.ClientSecret, credentials, basicAuth)

	// 请求体：grant_type=client_credentials
	reqBody := strings.NewReader("grant_type=client_credentials")

	req, err := http.NewRequest(http.MethodPost, url, reqBody)
	if err != nil {
		log.Printf("[getToken] 请求构造失败: %v", err)
		return "", 0, fmt.Errorf("构造 token 请求失败: %w", err)
	}
	req.Header.Set("Authorization", basicAuth)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	start := time.Now()
	resp, err := c.getHTTPClient().Do(req)
	if err != nil {
		log.Printf("[getToken] 请求失败: %v", err)
		return "", 0, fmt.Errorf("请求 token 接口失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[getToken] 读取响应失败: %v", err)
		return "", 0, fmt.Errorf("读取 token 响应失败: %w", err)
	}
	cost := time.Since(start)
	rawBody := strings.TrimSpace(string(body))
	log.Printf("[getToken] 原始响应: %s", rawBody)
	if resp.StatusCode != http.StatusOK {
		log.Printf("[getToken] 请求结束: HTTP %d, 耗时 %v", resp.StatusCode, cost)
		return "", 0, fmt.Errorf("token 接口返回状态码 %d，响应: %s", resp.StatusCode, rawBody)
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		log.Printf("[getToken] 请求结束: HTTP %d, 耗时 %v, JSON 解析失败: %v, 原始响应: %s",
			resp.StatusCode, cost, err, rawBody)
		return "", 0, fmt.Errorf("token 响应解析失败: %w", err)
	}
	if tokenResp.AccessToken == "" {
		log.Printf("[getToken] 请求结束: HTTP %d, 耗时 %v, access_token 为空, 原始响应: %s",
			resp.StatusCode, cost, rawBody)
		return "", 0, fmt.Errorf("token 接口返回的 access_token 为空")
	}

	ttl := time.Duration(tokenResp.ExpiresIn) * time.Second
	log.Printf("[getToken] 请求结束: HTTP %d, 耗时 %v, access_token=%s, expires_in=%ds",
		resp.StatusCode, cost, tokenResp.AccessToken, tokenResp.ExpiresIn)
	return tokenResp.AccessToken, ttl, nil
}

// get 发起 GET 请求，Authorization 头为 Bearer + access_token（由 EnsureToken 获取）
// path 为完整路径（含 /api/sync/orgs 等），params 为查询参数（目前写死为 "1"）
func (c *Client) get(fullURL string) ([]byte, error) {
	token, err := c.EnsureToken()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("构造请求失败: %w", err)
	}
	// 业务接口的 Authorization 头为 Bearer + access_token
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.getHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 %s 失败: %w", fullURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 响应失败: %w", fullURL, err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		log.Printf("[token] 接口 %s 返回 401，access_token 已失效，清空缓存待重新获取", fullURL)
		c.InvalidateToken()
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("接口 %s 返回状态码 %d，响应: %s", fullURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// getWithRetry 业务请求失败时，刷新 access_token 重试一次
func (c *Client) getWithRetry(fullURL string) ([]byte, error) {
	body, err := c.get(fullURL)
	if err != nil {
		log.Printf("请求 %s 失败，刷新 access_token 后重试: %v", fullURL, err)
		c.InvalidateToken()
		if body, err = c.get(fullURL); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// JavaConfigResponse Java 平台拉取配置接口的响应结构
type JavaConfigResponse struct {
	Code      int              `json:"code"`
	Data      []JavaConfigItem `json:"data"`
	Msg       string           `json:"msg"`
	Success   bool             `json:"success"`
	Timestamp int64            `json:"timestamp"`
}

// JavaConfigItem Java 平台返回的单条配置
type JavaConfigItem struct {
	ConnectConfig string `json:"connectConfig"`
	ID            int    `json:"id"`
	Name          string `json:"name"`
}

// FetchBizConfig 从我们 Java 平台拉取全量初始配置
// GET {java_url}/v1/rsNpsAccessConfiguration/queryAll
// 解析响应中的 connectConfig 字段（JSON 字符串），返回 BizConfig
func (c *Client) FetchBizConfig(javaURL string) (*config.BizConfig, error) {
	url := javaURL + config.QueryCfgPath
	log.Printf("[fetchConfig] 开始从 RS 模块拉取初始配置: GET %s", url)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("构造请求失败: %w", err)
	}

	start := time.Now()
	resp, err := c.getHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 RS 模块配置接口失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	cost := time.Since(start)
	log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, 原始响应: %s", resp.StatusCode, cost, strings.TrimSpace(string(body)))

	if resp.StatusCode != http.StatusOK {
		log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, 响应: %s", resp.StatusCode, cost, strings.TrimSpace(string(body)))
		return nil, fmt.Errorf("RS 模块配置接口返回 %d，响应: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var javaResp JavaConfigResponse
	if err := json.Unmarshal(body, &javaResp); err != nil {
		log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, JSON 解析失败: %v, 响应: %s", resp.StatusCode, cost, err, strings.TrimSpace(string(body)))
		return nil, fmt.Errorf("解析 RS 模块响应失败: %w", err)
	}

	if !javaResp.Success {
		log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, success=false, msg=%s", resp.StatusCode, cost, javaResp.Msg)
		return nil, fmt.Errorf("RS 模块返回失败: %s", javaResp.Msg)
	}

	if len(javaResp.Data) == 0 {
		log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, data 为空", resp.StatusCode, cost)
		return nil, fmt.Errorf("RS 模块返回 data 为空")
	}

	// 取第一条记录的 connectConfig（JSON 字符串）
	connectConfigStr := javaResp.Data[0].ConnectConfig
	log.Printf("[fetchConfig] 请求结束: HTTP %d, 耗时 %v, connectConfig=%s", resp.StatusCode, cost, connectConfigStr)

	biz, err := config.BizConfigFromConnectConfig(connectConfigStr)
	if err != nil {
		return nil, fmt.Errorf("解析 connectConfig 失败: %w", err)
	}

	log.Printf("[fetchConfig] 拉取成功: clientId=%s, ssoUrl=%s, iamapiUrl=%s", biz.ClientID, biz.SSOURL, biz.IAMAPIURL)
	return biz, nil
}
