package nps

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"nps/internal/config"
)

// Org 组织机构信息参考结构
// 字段按接口文档定义，透传时以原始 JSON 为准，本结构仅用于日志与校验参考
type Org struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	ParentID    int64  `json:"parentId"`
	Ancestors   string `json:"ancestors"`
	OrderNum    int    `json:"orderNum"`
	Leader      string `json:"leader"`
	Phone       string `json:"phone"`
	Email       string `json:"email"`
	Status      string `json:"status"`
	Type        string `json:"type"`
	Creator     string `json:"creator"`
	CreateTime  string `json:"createTime"`
	Updater     string `json:"updater"`
	UpdateTime  string `json:"updateTime"`
	Remark      string `json:"remark"`
	OrgCode     string `json:"orgCode"`
	OrgType     string `json:"orgType"`
	OrgLevel    int    `json:"orgLevel"`
	OrgPath     string `json:"orgPath"`
	OrgFullName string `json:"orgFullName"`
	Enabled     bool   `json:"enabled"`
}

// FetchOrg 调用组织机构信息接口（分页），返回原始 JSON
// GET {iamapi_url}/api/sync/orgs?page=N&size=N&orderby=...&filters=...
func (c *Client) FetchOrg(page, size int, orderby, filters string) (json.RawMessage, error) {
	biz := c.cfgMgr.GetBiz()
	q := url.Values{}
	q.Set("page", strconv.Itoa(page))
	q.Set("size", strconv.Itoa(size))
	if orderby != "" {
		q.Set("orderby", orderby)
	}
	if filters != "" {
		q.Set("filters", filters)
	}
	u := biz.IAMAPIURL + config.OrgPath + "?" + q.Encode()
	body, err := c.getWithRetry(u)
	if err != nil {
		return nil, fmt.Errorf("获取组织机构信息失败(page=%d): %w", page, err)
	}
	return json.RawMessage(body), nil
}
