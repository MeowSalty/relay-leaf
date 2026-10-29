package config

// Example 返回文件模式配置预设的新副本。
func Example() []byte { return []byte(example) }

const example = `# 示例身份和模型名称；转发前请配置自己的上游。
# 下方客户端和上游的 secret_ref 均可改为 secret: "你的密钥"，两者只能填写一个。
# 直接填写的密钥不做变量替换，也不修剪空白；客户端密钥须为 32–512 字节的 ASCII Bearer token。
version: 1
mode: local
source: file
listeners:
  gateway: 127.0.0.1:17890
ui:
  enabled: false
client_auth:
  mode: api_key
  allow_gemini_query_key: false
  keys:
    local-client:
      enabled: true
      secret_ref: env:RELAYLEAF_CLIENT_KEY
      secret_version: v1
      allowed_routes: [example-responses]
      limits: {max_inflight: 4}
providers:
  example-provider:
    enabled: true
    origin: https://api.example.com
    credentials:
      example-key:
        enabled: true
        secret_ref: env:RELAYLEAF_UPSTREAM_KEY
        secret_version: v1
    endpoints:
      responses:
        protocol: openai.responses
        operation: create
        path_template: /v1/responses
        request_modes: [non_stream, stream]
        auth: {type: bearer}
    models:
      example-model:
        upstream_name: example-upstream-model
        bindings:
          responses:
            enabled: true
            credentials: [example-key]
            request_modes: [non_stream, stream]
            limits: {max_inflight: 4}
routes:
  example-responses:
    protocol: openai.responses
    operation: create
    public_model: example-assistant
    selection: {strategy: weighted}
    session_affinity: {enabled: false}
    resource_policy: {mode: stateless_pool}
    targets:
      - target: example-provider/example-model/responses
        priority: 10
        weight: 1
execution_defaults:
  max_retries: 0
  empty_response: {enabled: true}
health_defaults:
  circuit_breaker: {enabled: false}
limits:
  max_inflight: 32
  max_request_body_bytes: 16777216
  max_decompressed_body_bytes: 16777216
  max_total_body_buffer_bytes: 67108864
  max_total_response_buffer_bytes: 16777216
  queue_capacity: 0
  queue_wait_timeout: 2s
  max_runtime_state_entries: 10000
  max_session_bindings: 10000
  max_session_bindings_per_client: 1000
  max_session_id_bytes: 128
`
