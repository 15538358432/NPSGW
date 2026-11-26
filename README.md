# NPSGW 新警通数据同步网关

NPSGW 根据 RS 的指令或启动配置，从新警通分页拉取组织和用户，将记录转换为 RS 约定的 JSON 格式，再通过 NSQ 发布。Redis 保存业务连接配置和访问令牌。

```text
RS --拉取指令、配置变更（NSQ）--> NPSGW
RS <--组织、用户数据（NSQ）-------- NPSGW
NPSGW --配置查询（HTTP）----------> RS
NPSGW --获取令牌、拉取数据（HTTP）--> 新警通
```

## 启动流程

1. 解析 `-config`，读取并校验 YAML。默认读取进程工作目录下的 `config.yaml`。
2. 根据 `log.dir` 创建日志目录，打开 `nps.log` 并启用文件滚动。
3. 连接 Redis 并读取 `nps_config`。该 Hash 不存在时，用 YAML 的 `nps` 段初始化并保存。
4. 如果配置了 `java_url`，向 RS 请求 `GET {java_url}/v1/rsNpsAccessConfiguration/queryAll`。当前代码取返回列表第一条记录的 `connectConfig`，将其中非空的连接字段写入 Redis 和内存。请求失败时记录错误并继续使用已有配置。
5. 调用 `EnsureToken()` 预热访问令牌，查找顺序为内存、Redis 的 `nps_token`、新警通 token 接口。预热失败时记录错误，后续拉取前再尝试获取。
6. 初始化 NSQ producer。如果 `pull_on_start: true`，在后台主动同步一轮，先组织、后用户，组织失败时仍尝试用户同步。
7. 订阅 RS 的拉取和配置变更 topic，并启动 HTTP API。服务在收到停止信号前持续消费消息。

`pull_on_start` 控制启动时的数据同步。启动时的配置拉取由 `java_url` 控制，token 预热在两种模式下都会尝试。

RS 的 HTTP 配置接口应返回 `success: true` 和非空的 `data` 数组，其中 `data[0].connectConfig` 是包含连接字段的 JSON 字符串。NSQ 配置变更消息中的 `connectConfig` 使用 JSON 对象。

## 配置文件

下方示例可用来准备部署配置。请按实际环境填写地址、凭据、平台标识和 NSQ 名称。

```yaml
redis:
  addr: "127.0.0.1:6379"
  password: ""
  db: 0

nsq:
  nsqd_addr: "127.0.0.1:4150"
  topic_pull: "rs_pull_nps_data"
  topic_access_cfg: "rs_sdk_access_configuration"
  topic_org: "sdk_organ_info"
  topic_user: "sdk_user_info"
  pull_channel: "nps"

api:
  port: 8080
java_url: "http://rs.example.com"
platform: "XJNps"
pull_on_start: true

sync:
  orgs:
    page_size: 10
  users:
    page_size: 10
  push_organInfo_all: true

log:
  dir: "/home/logs/nps"
  max_size_mb: 10
  max_files: 10

nps:
  sso_url: "https://sso.example.com"
  iamapi_url: "https://iam.example.com"
  client_id: "example-client"
  client_secret: "example-secret"
  insecure_skip_verify: false
```

| 配置 | 用途 |
| --- | --- |
| `redis` | 地址、密码及数据库编号，用于存储业务配置和 token。 |
| `nsq` | nsqd 地址、四个 topic 和订阅 channel 均从启动配置读取。实际名称以部署配置为准。 |
| `api.port`、`java_url` | 网关 HTTP 端口默认为 8080。可省略 `java_url`，以跳过从 RS 拉取配置。 |
| `platform` | 匹配配置变更消息中的 `platform`，同时填充对 RS 发布数据时的 `platformId`。 |
| `sync.orgs`、`sync.users` | 分别设置 `page_size`、可选的 `orderby` 和 `filters`。页大小缺省或小于等于 0 时取 50。 |
| `sync.push_organInfo_all` | 为 `true` 时合并所有组织页后一次发布，为 `false` 时按页发布。用户始终按页发布。 |
| `nps` | `nps_config` 不存在时使用的初始配置，其中 `sso_url` 用于取 token，`iamapi_url` 用于数据接口。 |
| `log` | 日志目录、单文件大小和保留文件总数。详见日志说明。 |

服务启动时加载 YAML。修改日志、分页、NSQ 等启动配置后重启服务即可。RS 更新的业务连接字段写入 Redis 和内存。

## 运行期间的触发方式

| 入口 | 动作 |
| --- | --- |
| RS 发布到 `nsq.topic_pull` | 根据消息中的 `param` 执行一次对应资源的分页同步。 |
| RS 发布到 `nsq.topic_access_cfg` | 匹配本地 `platform` 后解析 `connectConfig`，将其中非空的连接字段写入 Redis 和内存。 |
| `GET /api/config/fetch` | 手动触发一次 RS 配置查询并更新网关业务配置。需要设置 `java_url`。 |
| `GET /api/health` | 返回 `{"status":"ok"}`，用于确认网关 HTTP 接口可访问。 |

拉取指令示例为 `{"param":"organ"}` 和 `{"param":"user"}`。当前的拉取指令按 `param` 选择资源，平台匹配用于配置变更消息。配置变更消息使用下列结构，其中 `type` 用于日志记录。

```json
{
  "platform": "XJNps",
  "type": "update",
  "connectConfig": {
    "clientId": "example-client",
    "clientSecret": "example-secret",
    "ssoUrl": "https://sso.example.com",
    "iamapiUrl": "https://iam.example.com"
  }
}
```

NSQ 消费者在处理函数成功返回后确认消息，返回错误时请求延迟 5 秒重投，最多尝试 3 次。被忽略的消息会确认。启动主动同步单独运行一轮，失败会记录日志，后续可由 RS 指令触发。

## 数据拉取、转换和发布

1. **获取 token。** 缓存没有可用令牌时，通过 `POST {sso_url}/oauth2/token` 请求，使用客户端凭据进行 Basic 认证，表单参数为 `grant_type=client_credentials`。成功响应为 HTTP 200，最外层包含非空 `access_token`，可选的 `expires_in` 单位为秒，缺省缓存时长为 300 秒。
2. **分页请求。** 携带 `Authorization: Bearer <token>`，请求 `GET {iamapi_url}/api/sync/orgs` 或 `GET {iamapi_url}/api/sync/users`。每种资源从 `page=1` 开始，`size` 取各自的 `page_size`。满页时等待 200 ms 再请求下一页，返回空页或不足一页时结束。联调时若总数据正好 10 条且页大小为 10，第 2 页应返回空数组。
3. **提取和映射。** 兼容纯数组、单对象和 `data` 包装的返回。组织保留 `id`、`name`、`code`、`parentId`。用户保留 `id`、`name`、`code`，将 `orgId`、`orgName` 转为 `organId`、`organName`。两种资源均将布尔型 `enabled` 转为 `enable`，将 `createdAt`、`updatedAt` 转为 `createdTime`、`updatedTime`。时间转换成功时输出 `yyyy-MM-dd HH:mm:ss`，解析失败时保留原值并记录错误。
4. **封装和发布。** 组织发到 `nsq.topic_org`，用户发到 `nsq.topic_user`，消息外层为 `platformId` 和 `data`。下面是组织消息示例。

```json
{
  "platformId": "XJNps",
  "data": [
    {
      "id": "org-001",
      "name": "某组织",
      "code": "ORG001",
      "parentId": null,
      "enable": true,
      "createdTime": "2026-11-05 09:00:00",
      "updatedTime": "2026-11-05 10:00:00"
    }
  ]
}
```

## 日志和排查

- 网关在启动时自动创建 `log.dir`，将日常运行日志和带 `[ERROR]` 标记的错误日志追加到 `nps.log`。程序默认目录是 `/home/logs/nps`，以实际配置的 `log.dir` 为准。
- `log.max_size_mb` 以 MiB 为单位，`log.max_files` 包含当前日志。两项默认均为 10，可设置为 1 至 10。滚动后最新归档为 `nps.log.1`，编号越大越旧，超过保留数量时删除最旧的归档。
- `[NSQ] 开始发布` 会打印目标 topic、条数、大小及转换后的完整 JSON，`[NSQ] 发布完成` 表示 NSQ 发布成功。日志可能包含业务数据和凭据信息，共享前请脱敏。
- 按示例目录可用 `tail -F /home/logs/nps/nps.log` 跟踪日志，或用 `grep -F '[ERROR]' /home/logs/nps/nps.log` 筛选错误。若配置加载或日志初始化失败，可用 `journalctl -u nps -n 100 --no-pager` 查看 systemd 启动诊断。

## 打包与部署

1:：在IDEA终端或者 Windows 的项目目录中执行 `./build.bat`。脚本会先运行 `go vet`，再构建 Linux 二进制文件 `nps_amd`（x86_64）和 `nps_arm`（aarch64）。以服务器上 `uname -m` 的结果为准；

2：将配置文件和打好的包放在指定路径下，一般放在 /opt/pcs/nps下；

3：将服务配置文件nps.service放在/etc/systemd/system路径下，为打好的服务包赋权限  chmod +x nps_arm

4：执行systemctl start nps.service 启动服务