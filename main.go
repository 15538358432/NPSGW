package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"nps/internal/api"
	"nps/internal/config"
	"nps/internal/nsqx"
	"nps/internal/store"
	"nps/internal/syncer"
	"nps/internal/nps"
)

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// 加载启动配置（Redis/NSQ/API/Java 地址，修改需重启）
	startup, err := config.LoadStartup(*configPath)
	if err != nil {
		log.Fatalf("加载启动配置失败: %v", err)
	}

	// 连接 Redis
	redisStore, err := store.NewRedis(startup.Redis, "nps_config")
	if err != nil {
		log.Fatalf("连接 Redis 失败: %v", err)
	}
	defer redisStore.Close()

	// 创建配置管理器（从 Redis 加载业务配置，无则用 config.yaml 的 nps 段初始化）
	initialBiz := &startup.NPS
	cfgMgr, err := config.NewManager(startup, redisStore, initialBiz)
	if err != nil {
		log.Fatalf("创建配置管理器失败: %v", err)
	}

	client := nps.NewClient(cfgMgr, redisStore)

	// 启动时从 Java 平台拉取全量初始配置（如果配置了 java_url）
	if startup.JavaURL != "" {
		log.Printf("[init] 正在从 RS 模块拉取初始配置: %s%s", startup.JavaURL, config.QueryCfgPath)
		biz, err := client.FetchBizConfig(startup.JavaURL)
		if err != nil {
			log.Printf("[init] 拉取初始配置失败，将使用 Redis/配置文件中的现有配置: %v", err)
		} else {
			fieldMap := map[string]string{
				"clientId":     biz.ClientID,
				"clientSecret": biz.ClientSecret,
				"ssoUrl":       biz.SSOURL,
				"iamapiUrl":    biz.IAMAPIURL,
			}
			if err := cfgMgr.UpdateFields(fieldMap); err != nil {
				log.Printf("[init] 保存初始配置到 Redis 失败: %v", err)
			} else {
				log.Printf("[init] 初始配置拉取成功并保存到 Redis")
			}
		}
	} else {
		log.Printf("[init] 未配置 java_url，跳过从 RS 模块拉取初始配置")
	}

	// 预热 token
	if _, err := client.EnsureToken(); err != nil {
		log.Printf("启动时获取平台 token 失败，将在处理消息时重试: %v", err)
	} else {
		log.Printf("平台 token 已就绪，Redis key: %s", config.TokenKey)
	}

	// producer
	log.Printf("NSQ 已配置，正在连接 nsqd(%s)...", startup.NSQ.NSQDAddr)
	producer, err := nsqx.NewProducer(startup.NSQ.NSQDAddr)
	if err != nil {
		log.Fatalf("连接 NSQ producer 失败: %v", err)
	}
	log.Printf("NSQ producer 连接成功，组织 topic=%s, 用户 topic=%s", config.TopicOrg, config.TopicUser)
	defer producer.Stop()

	s := syncer.New(client, producer, cfgMgr, startup.Platform, startup.Sync)
	log.Printf("[init] 平台标识: %s", startup.Platform)
	log.Printf("[init] 分页配置: orgs(page_size=%d, orderby=%q, filters=%q), users(page_size=%d, orderby=%q, filters=%q)",
		startup.Sync.Orgs.GetPageSize(), startup.Sync.Orgs.Orderby, startup.Sync.Orgs.Filters,
		startup.Sync.Users.GetPageSize(), startup.Sync.Users.Orderby, startup.Sync.Users.Filters)

	// 启动后主动拉取一次全量组织/用户数据（异步执行，不阻塞启动流程）
	go s.SyncAll()

	// 订阅 rs 模块的 pull 指令
	consumer, err := nsqx.NewConsumer(
		startup.NSQ.NSQDAddr,
		config.TopicPull,
		config.PullChannel,
		s.HandlePull,
	)
	if err != nil {
		log.Fatalf("订阅 NSQ pull_topic(%s) 失败: %v", config.TopicPull, err)
	}
	defer consumer.Stop()

	// 订阅 rs 模块的配置变更
	cfgConsumer, err := nsqx.NewConsumer(
		startup.NSQ.NSQDAddr,
		config.TopicAccessCfg,
		config.PullChannel,
		s.HandleAccessConfig,
	)
	if err != nil {
		log.Fatalf("订阅 NSQ 配置变更 topic(%s) 失败: %v", config.TopicAccessCfg, err)
	}
	defer cfgConsumer.Stop()

	// 启动 HTTP 配置接口
	apiServer := api.NewServer(cfgMgr, client, startup.JavaURL)
	go func() {
		if err := apiServer.ListenAndServe(startup.API.Port); err != nil {
			log.Fatalf("HTTP API 服务启动失败: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("NPSGW 已就绪，等待 rs 模块消息... (拉取指令: %s, 配置变更: %s, HTTP 接口: :%d)",
		config.TopicPull, config.TopicAccessCfg, startup.API.Port)
	<-stop
	log.Println("收到退出信号，服务停止")
}
