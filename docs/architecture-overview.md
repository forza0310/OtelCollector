# SigNoz OTel Collector 架构总览

这份文档梳理本 fork 的 collector 在 SigNoz 部署中的**通信信道、配置生效路径、模块裁剪边界**——回答"它到底怎么和 SigNoz 后端协作"这类问题。

适合在以下场景查阅：
- 排查 collector 行为与镜像/config 不一致的怪事
- 决定要不要在 config 里启用某个新模块
- 评估再做一轮精简会断哪些用例

参考 commit：`78be1b3` 时的代码状态。下文所有文件路径都指向本仓。

---

## 1. OpAMP 是什么？

**Open Agent Management Protocol** —— OpenTelemetry 社区维护的 agent 远控协议，规范见 [open-telemetry/opamp-spec](https://github.com/open-telemetry/opamp-spec)。让一台中心化的 server 统一管理一群分散的 agent（在我们这里 agent = otel collector）。

### 协议层面

| 维度 | 取值 |
|---|---|
| 传输层 | WebSocket（也支持 HTTP polling，本仓写死 WebSocket） |
| 编码 | Protobuf |
| 方向 | 双向，长连接 |
| 客户端库 | `github.com/open-telemetry/opamp-go/client` |

本仓只是基于上游 lib 实现回调层和配置持久化，**不自己实现协议栈**：
- 客户端创建：[opamp/server_client.go:86](../opamp/server_client.go#L86) `client.NewWebSocket(...)`
- Server endpoint 解析：[opamp/config.go](../opamp/config.go) （从 `--manager-config` YAML 取 `server_endpoint`）

### 启用的 Capabilities

[opamp/server_client.go:164-168](../opamp/server_client.go#L164-L168)：

```go
ReportsStatus | AcceptsRemoteConfig | ReportsRemoteConfig |
ReportsEffectiveConfig | ReportsHealth
```

| 能力 | 含义 |
|---|---|
| `ReportsStatus` | 上报"我活着、当前状态如何" |
| `AcceptsRemoteConfig` | **接收 server 下发的配置** |
| `ReportsRemoteConfig` | 上报"我现在用的是哪个 remote config（hash）" |
| `ReportsEffectiveConfig` | 上报"实际生效的 config 全文"（debug 用） |
| `ReportsHealth` | 上报组件健康状态 |

**未启用**：`AcceptsPackages`（下发二进制/OTTL 脚本包）、`AcceptsRestartCommand`（远程重启）、`AcceptsOpAMPConnectionSettings`（server 重定向）。

### Nop bootstrap

[opamp/server_client.go:182-191](../opamp/server_client.go#L182-L191) 有个独到设计——collector 启动**不直接加载用户 config**，而是先用一个 "全 nop" 退化 config 启动，等 OpAMP server 下发真实 config 再重启子进程。

`initialNopConfig` 在 [opamp/server_client.go:202-249](../opamp/server_client.go#L202-L249)：把所有 pipeline 的 receivers 全替换成 `nop`、所有 exporters 全替换成 `nop`、processors 清空。

**目的**：让 k8s/docker healthcheck 在 collector 还没连上 OpAMP 时也能探活成功（"healthy but idle"）。

### Config 应用流程

```
1. main.go 启动，--manager-config 不为空
   → 进入 OpAMP 模式

2. copyConfigFile(--config, --copy-path)
   → main.go:60，把 --config 内容首次复制到 --copy-path

3. 此后 collector 的 --config 永远指向 --copy-path
   → 这就是为什么"镜像内置 /etc/otel/config.yaml 不生效"
   → 它只是首次拷贝给 /var/tmp/collector-config.yaml 的"种子"
   → 启动后立刻被 nop config 覆盖
   → 等 OpAMP server 推真实 config 时再次覆盖

4. OpAMP server 通过 WebSocket 推 AgentRemoteConfig
   → onRemoteConfigHandler (server_client.go:273)
   → configManager.Apply 算 hash、判断是否变化
   → 变化则写入 copyPath + 调 coll.Restart()
   → 失败时回滚（server_client.go:317-336）
```

### 关闭 OpAMP 接管

两种方式：

1. **启动时不传 `--manager-config`** → [main.go:59](../cmd/signozotelcollector/main.go#L59) 判空跳过，走 `simpleClient`（[opamp/simple_client.go](../opamp/simple_client.go)），collector 加载本地 `--config` 后**永远不接受远程配置**。
2. 传了但 server 不可达 → collector 困在 nop 模式。**生产不要这么干。**

---

## 2. SigNoz ↔ collector 通信信道全集

按"是否经 ClickHouse 中转"分两类。

### 直接通信（不走 CH）

| 通道 | 协议 | 方向 | 干什么 | 代码 |
|---|---|---|---|---|
| **OpAMP** | WebSocket (Protobuf) | 双向，长连接 | 下发 config / 上报状态、健康、effective config | [opamp/server_client.go](../opamp/server_client.go) |

**就这一条**。SigNoz 后端进程和 collector 进程之间，**没有任何其他直接的进程间通信**。

### 经 ClickHouse 间接通信（数据/元数据共享）

不是协议，是数据库表共享：

| 数据流 | 写入方 | 读取方 | 库.表 |
|---|---|---|---|
| traces 主数据 | collector (`clickhousetraces` exporter) | SigNoz UI 后端 | `signoz_traces.signoz_index_v3` 等 |
| metrics 主数据 | collector (`signozclickhousemetrics`) | SigNoz UI 后端 | `signoz_metrics.*` |
| logs 主数据 | collector (`clickhouselogsexporter`) | SigNoz UI 后端 | `signoz_logs.distributed_logs_v2` |
| **metadata**（资源属性、service 列表、attribute keys 字典）| collector (`metadataexporter`) | SigNoz UI 后端 | `signoz_metadata.*` |
| **meter**（用量/计费指标）| collector (`signozclickhousemeter`，经 `signozmeter` connector 聚合) | usage 模块 / SigNoz Cloud 计费 | `signoz_meter.*` |
| span metrics（RED 指标，由 spans 衍生）| collector (`signozspanmetricsprocessor` → `signozclickhousemetrics`) | SigNoz UI 后端（Service Map 等） | `signoz_metrics.*` |

这就是为什么"OpAMP 下发 config"里每个 pipeline 都扇出 3 个 exporter——SigNoz 后端**需要**这 3 类数据都到 CH 才能正常工作。

### Schema 同步（不是运行时通信，但属于 collector ↔ CH 协作）

Collector 二进制内置了 schema migrator 引擎（[cmd/signozschemamigrator/schema_migrator/](../cmd/signozschemamigrator/schema_migrator/)，经 [cmd/signozotelcollector/migrate/](../cmd/signozotelcollector/migrate/) 暴露为 `migrate` 子命令），干 DDL 同步：

| 子命令 | 作用 |
|---|---|
| `migrate bootstrap` | 建库 |
| `migrate sync up` | 同步表结构（DDL） |
| `migrate sync check` | 启动前自检表结构是否匹配代码期望 |
| `migrate async up` | 后台异步迁移大表 |

部署里通常作为独立 service（`signoz-telemetrystore-migrator`）跑。**升级 collector 镜像 = 升级 SigNoz 期望的表结构定义。**

### ⚠️ 谁能改 collector 运行时行为？

**只有 OpAMP**。其他都是单向数据流，不下指令。如果出现"我的 collector 行为变了但我没改任何东西"，第一个怀疑对象是 OpAMP server 下发了新 config。

---

## 3. 精简后哪些 pipeline 选项不能用了？

精简删除了 73 个模块工厂（fork 起点 `81062bf` → HEAD）。**对 OpAMP 实际下发的 config 无影响**（13 个被引用模块全部保留），但**限制了 config 里可以写什么**。

如果 config 引用被删模块，collector 启动 fatal：

```
error decoding 'exporters': unknown type: "prometheus" for id: "prometheus"
```

OpAMP 模式下：reload 失败会回滚到上一版 config（[server_client.go:317-336](../opamp/server_client.go#L317-L336)），collector 不会崩溃。

### 删除模块的能力损失清单

按用例分类：

| 用例 | 删掉的模块 | 仍可用的替代 |
|---|---|---|
| Kafka / Pulsar / RabbitMQ / Kinesis / S3 / GCP PubSub 出站 | kafkaexporter, pulsarexporter, rabbitmqexporter, awskinesisexporter, awss3exporter, googlecloudpubsubexporter | **`signozkafkareceiver` 入站还在；出站没了** |
| Jaeger / Zipkin 协议接收 traces | jaegerreceiver, zipkinreceiver | OTLP only |
| Filelog / Syslog / TCP / UDP 等纯日志通道 | filelogreceiver, syslogreceiver, tcplogreceiver, udplogreceiver | OTLP logs only |
| HTTP / TCP / ICMP / SSH / SNMP / Netflow 主动健康检查 | httpcheckreceiver, tcpcheckreceiver, icmpcheckreceiver, sshcheckreceiver, snmpreceiver, netflowreceiver, ntpreceiver, chronyreceiver | 全没了 |
| 云厂商/平台专属遥测拉取 | githubreceiver, gitlabreceiver, dockerstatsreceiver, podmanreceiver, jmxreceiver, redfishreceiver, nsxtreceiver, expvarreceiver, ciscoosreceiver, envoyalsreceiver, k8sobjectsreceiver, otelarrowreceiver, osqueryreceiver, otlpjsonfilereceiver, simpleprometheusreceiver, systemdreceiver, webhookeventreceiver, windowsperfcountersreceiver, windowsservicereceiver, filestatsreceiver, yanggrpcreceiver, pprofreceiver | OTLP / hostmetrics / kubeletstats / k8scluster / prometheus 还在 |
| 写 Prometheus / 文件 / syslog / Alertmanager / Cassandra / Datadog / Zipkin | prometheusexporter, prometheusremotewriteexporter, fileexporter, syslogexporter, alertmanagerexporter, cassandraexporter, zipkinexporter, datadogconnector, grafanacloudconnector | **全没了**。出站只能 OTLP/OTLPHTTP/debug/nop/clickhouse* |
| OAuth2 / OIDC / Azure 三种 auth extension | oauth2clientauthextension, oidcauthextension, azureauthextension | basicauth + bearertoken 还在 |
| 轮询/负载均衡/异常追踪 connector | failoverconnector, loadbalancingexporter, roundrobinconnector, exceptionsconnector, slowsqlconnector, metricsaslogsconnector, otlpjsonconnector, sumconnector, countconnector, datadogsemanticsprocessor | 主路径 connector（forward/routing/servicegraph/signaltometrics/spanmetrics/signozmeter）都保留 |
| 高级 processor（去重 / 异常 / 隔离森林 / DNS / GeoIP / Schema） | intervalprocessor, isolationforestprocessor, dnslookupprocessor, geoipprocessor, schemaprocessor, sumologicprocessor, coralogixprocessor, remotetapprocessor, groupbytraceprocessor, unrollprocessor | 数据增强四件套（attributes/resource/transform/filter）都在；`logdedupprocessor` 已保留 |
| Docker / ECS observer | dockerobserver, ecsobserver, ecstaskobserver | hostobserver + k8sobserver 还在 |
| Jaeger Remote Sampling 下发 extension | jaegerremotesampling | tailsampling 等"本地决策"的还在 |

### 仍可用的核心能力

OTLP gRPC/HTTP 进 → batch/memorylimiter/attributes/resource/transform/filter/tail_sampling/signozspanmetrics 处理 → ClickHouse traces/logs/metrics/meter/metadata 出 + Prometheus 进/scrape，**这是 SigNoz 主链路全程**，精简过程严格保留。

---

## 4. config 控制什么？怎么生效？

### YAML 顶层结构

```yaml
receivers:    # 数据从哪进
exporters:    # 数据往哪走
processors:   # 中间怎么转换
connectors:   # 跨 pipeline 桥：一个 pipeline 的 exporter，同时是另一个的 receiver
extensions:   # 不进数据流，给整个 collector 提供能力（auth/health/zpages）

service:
  extensions: [...]   # 启用哪些 extension
  telemetry:          # collector 自己的 log/metrics
    logs: ...
    metrics: ...
  pipelines:          # 真正的数据图
    <signal>/<name>:
      receivers: [...]
      processors: [...]     # 顺序敏感！
      exporters: [...]
```

Pipeline key 前缀决定 signal 类型：`traces` / `metrics` / `logs` / `profiles`。斜杠后是任意 label（如 `metrics/prometheus`）。同 signal 类型的多个 pipeline 互不干扰，各跑各的。

### Pipeline 内部数据流

```
   ┌──────────┐   ┌────────┐   ┌────────┐   ┌──────────┐
   │ receiver │ → │ proc 1 │ → │ proc N │ → │ exporter │
   └──────────┘   └────────┘   └────────┘   └──────────┘
                    (顺序执行，每个 proc 看到上个 proc 的输出)
```

- **多 receiver**：每个 receiver 都把数据塞到同一组 processor 链入口（fan-in）。
- **多 exporter**：链尾把数据 fan-out 给每个 exporter（**复制 pdata 对象，每个 exporter 独立处理**——这就是为什么 OpAMP 那份 config 里 traces pipeline 写 `[clickhousetraces, metadataexporter, signozmeter]` 会让流量乘以 3）。
- **processor 顺序敏感**：`batch` 放在 `signozspanmetrics/delta` 之后 vs 之前结果不同。

### 每段配置控制什么行为

#### receivers
入站协议、监听端口、采集间隔：

```yaml
otlp:
  protocols:
    grpc: {endpoint: 0.0.0.0:4317}
    http: {endpoint: 0.0.0.0:4318}
hostmetrics:
  collection_interval: 30s
  scrapers: {cpu:, memory:, disk:}
prometheus:
  config:
    scrape_configs: [...]              # 内嵌完整 Prometheus scrape 语法
```

#### processors
单条数据/批的转换。常见类型：

| 类型 | 作用 |
|---|---|
| `batch` | 攒批，控制下游 RPC 频率/大小 |
| `memory_limiter` | 内存压力时主动反压 |
| `attributes` / `resource` | 增改属性 |
| `filter` | 按条件丢弃 |
| `transform` (OTTL) | 通用转换语言 |
| `tail_sampling` / `signoztailsampler` | 整条 trace 决策保留/丢弃 |
| `signozspanmetrics` | 从 trace 衍生 RED 指标 |

#### exporters
出站协议、目标地址、并发/重试/队列：

```yaml
clickhousetraces:
  datasource: tcp://clickhouse:9000/signoz_traces
  retry_on_failure: {enabled: true, initial_interval: 5s}
  sending_queue: {enabled: true, queue_size: 100, num_consumers: 5}
```

#### connectors
特殊"双面"组件——**对上游 pipeline 是 exporter，对下游 pipeline 是 receiver**：

```yaml
connectors:
  signozmeter:
    dimensions: [...]
    metrics_flush_interval: 1h

service.pipelines:
  traces:
    exporters: [..., signozmeter]       # 这里它是 exporter
  metrics/meter:
    receivers: [signozmeter]            # 这里它是 receiver
    exporters: [signozclickhousemeter]  # 输出真出口
```

用途：跨 signal 类型转换（traces → metrics）、流量分发、采样后再聚合。

#### extensions
**不参与数据流**，给 collector 进程提供副功能：

| 类型 | 作用 |
|---|---|
| `health_check` | HTTP 健康检查端点（默认 :13133） |
| `pprof` | Go pprof 端点（默认 :1777） |
| `zpages` | debug 网页（默认 :55679） |
| `basicauth` / `bearertoken` | 给 OTLP receiver 加认证 |

#### service.telemetry
**collector 自己的可观测性**——日志格式、内部指标暴露端点（默认 `:8888/metrics`）。OpAMP 那份 config 的 `prometheus` receiver 自抓的就是这个端点。

### config → 运行时：完整时序

```
1. 进程启动 (main.go)
   │
2. otelcol.NewConfigProvider({URIs: [configPath], ProviderFactories: [file, env]})
   │  ↑ 这里决定支持 ${env:FOO}、${file:/path} 这些插值语法
   │
3. provider.Get(ctx, factories)
   │  ↑ 第一步：把 YAML 解析成 *Config 结构体
   │    factories 来自 components.Components()
   │    每个 receivers/processors/.../extensions 下的 key 必须匹配某个 factory.Type()
   │    匹配不到 → "unknown type" 启动失败
   │  ↑ 第二步：对每段 YAML 调用 factory.CreateDefaultConfig() 得到默认值
   │    再 unmarshal 用户的 YAML 进去
   │    YAML 字段名不匹配 struct tag → "has invalid keys" 启动失败
   │
4. cfg.Validate()
   │  ↑ 每个 Config 实现 Validate()，校验端口/路径/必填字段
   │  ↑ Pipeline graph 校验：是否有孤儿 receiver/exporter？是否有循环？
   │     connector 的 from/to 是否合法？
   │
5. service.New(cfg, factories) → 构建数据图
   │  ↑ 对每个 pipeline，按 receiver→procs→exporter 顺序：
   │     - 调 factory.CreateXxxReceiver/Processor/...(ctx, settings, cfg, nextConsumer)
   │     - 每个组件都是一个长跑 goroutine（或一组）
   │     - 组件之间通过 consumer.Traces/Metrics/Logs 接口（强类型 channel）连接
   │  ↑ 内存里就是一张有向图，runtime 不再看 YAML
   │
6. service.Start(ctx)
   │  ↑ 按拓扑序启动：extensions → exporters → processors → receivers
   │    receiver 最后启动是因为它一启动数据就开始进，下游必须先就绪
   │
7. 运行时
   │  ↑ Receiver 收到数据 → pdata 对象（OTLP 内存模型）
   │  ↑ 调 nextConsumer.ConsumeTraces(ctx, td) 同步往下推
   │  ↑ batch processor 等会异步 buffer
   │  ↑ 多 exporter 时：fanout consumer 复制 pdata 给每个 exporter
   │  ↑ exporter 内部有 queue + retry，是异步的
   │
8. config 改了怎么办？
   │  ↑ OpAMP 模式：OpAMP server 推新 config → reload() → 写文件 + coll.Restart()
   │     Restart = 完整 shutdown + 新建 service.New(newCfg) + Start
   │     **不是热更新单个组件**，是整个 collector 子图重建
   │  ↑ 非 OpAMP 模式：没有热加载，必须重启进程
```

### 关键文件索引

| 关注点 | 文件 |
|---|---|
| 配置解析入口（otel 上游） | `go.opentelemetry.io/collector/otelcol/configprovider.go` |
| 图构建 | `go.opentelemetry.io/collector/service/internal/graph/` |
| 本仓的 collector 包装（提供 Restart 给 OpAMP 用） | [signozcol/](../signozcol/) |
| 本仓的 reload 协作 | [opamp/server_client.go:301-340](../opamp/server_client.go#L301-L340) |
| 注册的组件工厂表 | [components/components.go](../components/components.go) |
| 镜像内置 default config（仅作 OpAMP bootstrap 种子） | [conf/default.yaml](../conf/default.yaml) |
| 守护测试：default config 引用的模块都得在工厂表里 | [components/components_test.go](../components/components_test.go) |

---

## 一句话总结表

| 问题 | 一句话答案 |
|---|---|
| OpAMP 是什么 | OpenTelemetry 定义的 agent 远控协议，WebSocket+Protobuf，本仓只启用"接收远程配置 + 上报状态/健康"几个能力 |
| 还有什么通信 | **没有了**。SigNoz ↔ collector 直接信道只有 OpAMP；其他都是经 ClickHouse 表共享的数据 + 镜像里附带的 schema migrator DDL |
| 删了模块影响 config 吗 | 影响"可以写什么 pipeline"，但**不影响"现在 OpAMP 实际下发的那份 pipeline"**；启动时 config 引用不存在的模块会 fatal，OpAMP 模式下会自动回滚 |
| config 控制什么 / 怎么生效 | YAML 描述一张有向数据图，启动时按工厂表实例化 → 拓扑序启动各组件 → receiver→processor→exporter 同步链 + exporter 异步队列。**改 config 必须重启 collector 子进程**，OpAMP 自动帮你做 |

---

## 排查 checklist

`collector → ClickHouse 流量异常变化`：

1. **看实际生效 config**：`docker exec <collector> cat /var/tmp/collector-config.yaml`（不是 `/etc/otel/config.yaml`！）。
2. **看 collector 内部指标**：`curl :8888/metrics | grep -E "otelcol_receiver_accepted|otelcol_exporter_sent"`。比例不变 → 上游应用侧流量本身变了；比例变了 → collector 内部行为差异。
3. **看 OpAMP 是否在管**：`docker logs` 找 `"Applying default config"` / `"Effective config"` 类日志。
4. **看 SigNoz 后端版本**：compose 的 `signoz/signoz:$VERSION`——后端版本决定下发模板，不是 collector 镜像决定。
