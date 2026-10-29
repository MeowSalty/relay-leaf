package config

// Example 返回文件模式配置预设的新副本。
func Example() []byte { return []byte(example) }

const example = `# 支持 POST /v1/responses 的普通与 SSE 调用，使用公开模型和客户端 Bearer 密钥。
# 每个请求最多调用一次上游；空回检测确认没有有效输出时返回 502。
# 示例身份和模型名称；转发前请配置自己的上游。
# 下方客户端和上游的 secret_ref 均可改为 secret: "你的密钥"，两者只能填写一个。
# 直接填写的密钥不做变量替换，也不修剪空白；客户端密钥须为 32–512 字节的 ASCII Bearer token。
# 字段的完整取值约束见项目 README；此处注释只作概览。
# 下方三项为配置格式版本与来源，当前使用本地文件模式。
version: 1
mode: local
source: file
# 数据监听地址；端口被占用时启动失败，不会自动更换端口。
listeners:
  gateway: 127.0.0.1:17890
# 管理界面尚未开放，必须保持关闭。
ui:
  enabled: false
# 上游访问组：把凭据按用途分组，客户端与凭据通过名称互相引用；
# 凭据须命中客户端的某个允许组且不命中任何拒绝组，拒绝优先。
access_groups:
  general:
    name: 普通上游
# 客户端鉴权；keys 下每项是一个 Bearer 密钥，键名为调用方标识。
client_auth:
  mode: api_key
  # 经 URL 查询参数传递密钥的方式尚未开放。
  allow_gemini_query_key: false
  keys:
    local-client:
      enabled: true
      # 密钥材料来源，支持环境变量或文件引用；也可改为 secret 直接填写。
      secret_ref: env:RELAYLEAF_CLIENT_KEY
      # 材料代次标识；更换密钥材料时同步更新。
      secret_version: v1
      # 允许访问的公开路由；空列表表示拒绝全部路由。
      allowed_routes: [example-responses]
      # 允许使用的上游访问组；空列表表示拒绝全部上游。
      allowed_access_groups: [general]
      # 拒绝的上游访问组，优先于允许组；可省略，默认为空。
      denied_access_groups: []
      limits: {max_inflight: 4}
# 上游提供方：每个提供方声明服务地址、凭据、端点与模型。
providers:
  example-provider:
    enabled: true
    # 上游基地址，仅含协议与主机；请求路径由端点声明。
    origin: https://api.example.com
    credentials:
      example-key:
        enabled: true
        # 该凭据归属的访问组，必填且非空。
        access_groups: [general]
        secret_ref: env:RELAYLEAF_UPSTREAM_KEY
        secret_version: v1
    endpoints:
      responses:
        # 上游协议与操作；当前支持 openai.responses 的 create。
        protocol: openai.responses
        operation: create
        # 上游请求路径。
        path_template: /v1/responses
        # 支持的调用方式；non_stream 为普通请求，stream 为 SSE 流式。
        request_modes: [non_stream, stream]
        # 上游认证方式；bearer 表示注入 Bearer 密钥。
        auth: {type: bearer}
    models:
      example-model:
        # 上游真实模型名；转发时把公开模型名替换为该名称。
        upstream_name: example-upstream-model
        bindings:
          responses:
            enabled: true
            # 可服务该模型的凭据；多个凭据轮流使用。
            credentials: [example-key]
            request_modes: [non_stream, stream]
            limits: {max_inflight: 4}
# 公开路由：客户端请求的模型名在此精确匹配。
routes:
  example-responses:
    protocol: openai.responses
    operation: create
    # 客户端可见的模型名，与上游模型通过绑定对应。
    public_model: example-assistant
    # 目标选择策略；weighted 表示同级目标按权重随机分流。
    selection: {strategy: weighted}
    # 会话亲和尚未开放，保持关闭。
    session_affinity: {enabled: false}
    # 资源归属策略；stateless_pool 服务无状态请求，硬资源引用需使用 fixed_scope。
    resource_policy: {mode: stateless_pool}
    targets:
      # 目标按 提供方/模型/端点 三段命名。
      # priority 数值越小越优先，只有当前可用最高优先级参与选择；weight 决定同级分流比例。
      - target: example-provider/example-model/responses
        priority: 10
        weight: 1
# 请求执行默认值。
execution_defaults:
  # 自动重试尚未开放；0 表示每个请求最多一次真实上游调用。
  max_retries: 0
  # 空回检测；确认响应没有有效输出且尚未提交时返回 502。
  empty_response: {enabled: true}
health_defaults:
  # 自动熔断尚未开放，保持关闭；上游冷却与已确认限额仍独立生效。
  circuit_breaker: {enabled: false}
# 实例级资源上限。
limits:
  # 全实例最大并发请求数。
  max_inflight: 32
  # 请求体与响应缓冲的尺寸上限，单位为字节。
  max_request_body_bytes: 16777216
  max_decompressed_body_bytes: 16777216
  max_total_body_buffer_bytes: 67108864
  max_total_response_buffer_bytes: 16777216
  # 首次排队尚未开放，保持为 0。
  queue_capacity: 0
  queue_wait_timeout: 2s
  # 运行状态、会话绑定等内存记录的容量上限。
  max_runtime_state_entries: 10000
  max_session_bindings: 10000
  max_session_bindings_per_client: 1000
  max_session_id_bytes: 128
`
