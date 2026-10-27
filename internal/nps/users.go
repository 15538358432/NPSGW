package nps

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"nps/internal/config"
)

// User 用户信息参考结构
// 字段按接口文档定义，透传时以原始 JSON 为准，本结构仅用于日志与校验参考
type User struct {
	ID          int64  `json:"id"`
	UserName    string `json:"userName"`
	NickName    string `json:"nickName"`
	UserType    string `json:"userType"`
	Email       string `json:"email"`
	Phonenumber string `json:"phonenumber"`
	Sex         string `json:"sex"`
	Avatar      string `json:"avatar"`
	Status      string `json:"status"`
	DeptID      int64  `json:"deptId"`
	DeptName    string `json:"deptName"`
	OrgID       int64  `json:"orgId"`
	OrgName     string `json:"orgName"`
	PostCode    string `json:"postCode"`
	PostName    string `json:"postName"`
	PoliceNo    string `json:"policeNo"`
	IDCard      string `json:"idCard"`
	Creator     string `json:"creator"`
	CreateTime  string `json:"createTime"`
	Updater     string `json:"updater"`
	UpdateTime  string `json:"updateTime"`
	Remark      string `json:"remark"`
	Enabled     bool   `json:"enabled"`
}

// FetchUser 调用用户信息接口（分页），返回原始 JSON
// GET {iamapi_url}/api/sync/users?page=N&size=N&orderby=...&filters=...
func (c *Client) FetchUser(page, size int, orderby, filters string) (json.RawMessage, error) {
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
	u := biz.IAMAPIURL + config.UserPath + "?" + q.Encode()
	body, err := c.getWithRetry(u)
	if err != nil {
		return nil, fmt.Errorf("获取用户信息失败(page=%d): %w", page, err)
	}
	return json.RawMessage(body), nil
}
