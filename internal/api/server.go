package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"nps/internal/config"
	"nps/internal/nps"
)

// Server HTTP 配置接口服务
type Server struct {
	mgr     *config.Manager
	client  *nps.Client
	javaURL string
	mux     *http.ServeMux
}

// NewServer 创建 HTTP API 服务
func NewServer(mgr *config.Manager, client *nps.Client, javaURL string) *Server {
	s := &Server{mgr: mgr, client: client, javaURL: javaURL, mux: http.NewServeMux()}
	// GET  /api/config/fetch  手动从 RS 模块拉取全量初始配置
	s.mux.HandleFunc("/api/config/fetch", s.handleFetchConfig)
	// GET  /api/health        健康检查
	s.mux.HandleFunc("/api/health", s.handleHealth)
	return s
}

// ListenAndServe 启动 HTTP 服务
func (s *Server) ListenAndServe(port int) error {
	addr := fmt.Sprintf(":%d", port)
	log.Printf("[api] HTTP 接口已启动: http://0.0.0.0%s (GET=/api/config/fetch, GET=/api/health)", addr)
	return http.ListenAndServe(addr, s.corsMiddleware(s.mux))
}

// corsMiddleware 简单 CORS 支持
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleFetchConfig 从 RS 模块拉取全量初始配置
// GET /api/config/fetch
func (s *Server) handleFetchConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "仅支持 GET 请求")
		return
	}

	if s.javaURL == "" {
		writeError(w, http.StatusBadRequest, "java_url 未配置")
		return
	}

	biz, err := s.client.FetchBizConfig(s.javaURL)
	if err != nil {
		log.Printf("[api] 拉取初始配置失败: %v", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("拉取初始配置失败: %v", err))
		return
	}

	// 保存到 Redis
	fieldMap := map[string]string{
		"clientId":     biz.ClientID,
		"clientSecret": biz.ClientSecret,
		"ssoUrl":       biz.SSOURL,
		"iamapiUrl":    biz.IAMAPIURL,
	}

	if err := s.mgr.UpdateFields(fieldMap); err != nil {
		log.Printf("[api] 保存初始配置失败: %v", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("保存初始配置失败: %v", err))
		return
	}

	log.Printf("[api] 初始配置拉取并保存成功")
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"code":    200,
		"message": "初始配置拉取成功",
		"config":  s.mgr.GetBiz(),
	})
}

// handleHealth 健康检查
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]interface{}{
		"code":    status,
		"message": msg,
	})
}
