# RelayLeaf

本地即用、可自托管的轻量 AI 网关：按协议、模型和权限选择上游，保留原协议转发，并提供调度、健康管理和可观测性。

已提供配置模板生成、离线检查、秘密解析和文件模式启动。数据入口支持 `POST /v1/responses` 的普通及 SSE 流式调用，按公开模型和客户端授权选择配置的上游，保留原协议响应。传输与原字节样本的测试范围见 [测试样本说明](backend/internal/testkit/README.md)。

构建与验证使用 Go 1.26 系列，建议使用该系列的最新补丁版本；后端是独立 Go 模块，运行不需要数据库、前端工具或 CGO。

Task 按项目分层，需要安装 [Task 3](https://taskfile.dev/docs/installation)。根目录的 [Taskfile.yml](Taskfile.yml) 汇总项目入口与跨项目检查，后端命令由 [backend/Taskfile.yml](backend/Taskfile.yml) 维护。在仓库根目录执行：

```text
task
task backend:run '--' version
task backend:build
task format
task format:check
task backend:test
task check
task backend:test '--' -race
```

`task` 列出各项目命令；`task backend:build` 将无 CGO 的本地程序输出到 `build/`；`task backend:check` 依次执行后端格式检查、依赖校验、测试、静态检查、配置样例检查和构建。根目录的 `task check` 汇总各项目检查，包含后端检查。

`task format` 汇总各项目格式化，使用 Go 自带的 `gofmt` 格式化后端源码；`task format:check` 只检查格式，不改写文件，发现未格式化文件或语法错误时失败。后端也可单独执行 `task backend:format` 和 `task backend:format:check`。

`backend:run` 与 `backend:test` 接受 `--` 后的参数，分别传给程序和测试工具。后端任务的工作目录固定为 `backend/`，配置文件和其他相对路径也以该目录为基准。在 `backend/` 内执行任务时省略 `backend:` 前缀，例如 `task test` 或 `task format`。

示例为 `'--'` 加引号，兼容 PowerShell 下 npm/pnpm 的 `task.ps1` 入口，避免参数分隔符被移除。

也可直接使用 Go 命令，以下命令均在 `backend/` 执行：

```text
go run ./cmd/relayleaf version
go run ./cmd/relayleaf init --preset file --output relayleaf.yaml
go run ./cmd/relayleaf config check --config relayleaf.yaml
go run ./cmd/relayleaf config check --config relayleaf.yaml --resolve-secrets
go run ./cmd/relayleaf local --source file --config relayleaf.yaml
```

`init` 仅创建指定配置文件，目标存在时拒绝覆盖。客户端和上游密钥都可以通过 `secret` 直接填写，或通过 `secret_ref` 引用环境变量、文件；同一对象只能填写一个来源。模板默认使用 `env:RELAYLEAF_CLIENT_KEY` 和 `env:RELAYLEAF_UPSTREAM_KEY`，可设置这两个环境变量，也可将对应字段改为直接填写：

```yaml
# 将模板中对应的密钥对象改为以下写法，其他配置保持原样。
access_groups:
  general:
    name: 普通上游
client_auth:
  keys:
    local-client:
      secret: "rlk_example_0123456789abcdefghijklmnopqrstuv"
      secret_version: v1
      allowed_routes: [example-responses]
      allowed_access_groups: [general]
      denied_access_groups: []
providers:
  example-provider:
    credentials:
      example-key:
        access_groups: [general]
        secret: "sk-example-upstream-key"
        secret_version: v1
```

上例密钥是虚构示例，使用时替换为自己的材料，并删除同一对象原有的 `secret_ref`。轮换中的 `retiring_secret` 也支持相同的来源选择。`secret` 必须是非空 YAML 字符串，建议加引号；值不做环境变量替换、不解析 `env:`/`file:` 前缀，也不修剪空白。ClientKey 必须为 32–512 字节的 ASCII Bearer token，实际使用时应采用足够随机的材料；上游密钥须满足端点的认证格式。更换密钥材料时同步更新 `secret_version`。上游 origin 和模型名是示例，按实际服务修改。配置检查和启动检查不会请求上游。

普通 `config check` 校验已填写的 `secret` 及来源字段，不读取环境变量或密钥文件；`--resolve-secrets` 和启动检查按启用状态与有效期解析全部必需材料，并检查客户端密钥是否重复。两种检查的输出均不包含秘密值。`file:` 引用相对配置文件目录解析。无参数启动使用当前目录的 `relayleaf.yaml`；缺文件或端口占用明确失败，不切换配置来源，也不递增端口。默认数据监听为 `127.0.0.1:17890`，没有管理监听。正常停止退出码为 0，配置或启动错误为 1，参数错误为 2。

[最小文件样例](examples/relayleaf.file.yaml) 由同一模板生成。[多上游与资源路由样例](examples/relayleaf.routing.yaml) 展示两个 Provider、普通与编程访问组、同一公开模型的授权分流，以及仅普通客户端可用的固定资源路由。样例显式关闭自动重试、自动熔断、队列和 UI，保留默认空回检测。实例、客户端、目标和适用配额域支持并发限制。

已有配置需手工补充访问组后重新检查：在顶层 `access_groups` 声明组，为每个 Credential 填写必填、非空的 `access_groups`，并为每个 ClientKey 填写必填的 `allowed_access_groups`。`denied_access_groups` 可省略，默认为 `[]`。程序不会自动重写已有文件或给未分组的凭据添加公共组；禁用凭据、禁用或过期客户端也必须填写合法引用。上述内联片段包含这些字段。

ClientKey 先通过 `allowed_routes` 获得公开路由权限，再通过访问组收窄绑定的上游凭据。凭据须命中至少一个允许组，且不能命中任何拒绝组；deny 优先，即使同一凭据另有允许组，命中拒绝组仍排除整个凭据。`allowed_routes: []` 拒绝全部路由，`allowed_access_groups: []` 拒绝全部上游，不支持通配或隐式全部授权。禁用 Route 仍先匹配并检查授权；未获路由或上游组授权分别返回 `403/route_forbidden` 或 `403/upstream_forbidden`。合法配置可人工停用路由、全部目标或全部凭据；有权限且满足资源、模式和能力约束，但人工停用或运行限制使其不可执行时返回 `503/no_available_target`。

路由支持多个启用目标。`priority` 数值越小越优先，只有当前可用优先级中的目标参与选择；`weight` 只决定同级目标的随机分流比例，每个目标贡献一次权重，目标内合法凭据轮流使用。新样例的两个目标均为 priority=10、weight=3:1：普通客户端在两个可用目标间按这一比例分流，编程客户端只获准 primary 的编程凭据。Provider 名称 `backup` 本身不产生备用行为；需要严格主备时为它配置更大的 priority。

已识别的 previous_response_id、文件、向量库和容器等硬资源引用必须通过 `fixed_scope` 路由限定到显式声明的 Provider/ResourceScope，并由同一作用域覆盖本次全部资源类型。`stateless_pool` 不能猜测硬引用归属。固定资源路由只选择同时满足访问组授权的作用域成员；`private` 由一个凭据引用，不保证不同客户端的资源所有权隔离。opaque reasoning/compaction 还要求组授权与作用域求交后的静态候选使用相同有效上游模型名，人工禁用或运行限制不能免除模型冲突。

多上游样例使用虚构的 origin、项目和上游模型；使用前替换为真实值，确保 ResourceScope.owner 与凭据的 identity.attributes/selectors 一致，并核实相应资源访问能力。启动前设置五个环境变量：`RELAYLEAF_GENERAL_CLIENT_KEY`、`RELAYLEAF_CODING_CLIENT_KEY`、`RELAYLEAF_PRIMARY_GENERAL_KEY`、`RELAYLEAF_PRIMARY_CODING_KEY`、`RELAYLEAF_BACKUP_GENERAL_KEY`。前两个为不同的客户端 Bearer 密钥，后三个为对应上游材料。在 `backend/` 执行：

```text
go run ./cmd/relayleaf config check --config ../examples/relayleaf.routing.yaml
go run ./cmd/relayleaf config check --config ../examples/relayleaf.routing.yaml --resolve-secrets
go run ./cmd/relayleaf local --source file --config ../examples/relayleaf.routing.yaml
```

每个请求最多发起一次真实上游调用。空回检测仅在请求允许重放时暂存响应；确认没有有效输出时返回 `502/upstream_empty_response`，出现有效输出后立即交付。未知事件、观察到限或观察不受支持时降级透传；响应已提交后发生错误则终止传输。

目前只开放 Responses create 的普通与 SSE 调用，以及显式声明的 `image_input`、`tool_calling`、`file_input`、`server_tools` 能力。服务器工具及其历史调用/输出续接不授予重放资格，空回检测不为这些请求暂存或改写响应，直接透传上游结果。图像遮罩中的文件引用可被识别，但不开放 `image_output`。其他协议入口、Responses compact、后台创建、自动重试、自动熔断、队列、会话亲和、管理界面及音视频输入输出尚未开放。

上游冷却独立于自动熔断：429 的限制范围不明时只限制当前凭据，无有效 `Retry-After` 时使用 `quota_cooldown`，到期仅放行一个恢复尝试。已确认的余额或消费额度耗尽会保持限制，修复额度后需重启进程重新加载配置。普通 401/403/404 不被推断为永久失效。

完成上游配置并启动后，以下 PowerShell 示例调用模板中的公开模型：

```powershell
$requestHeaders = @{ Authorization = "Bearer $env:RELAYLEAF_CLIENT_KEY" }
$requestBody = '{"model":"example-assistant","input":"你好","stream":false}'
Invoke-RestMethod -Uri http://127.0.0.1:17890/v1/responses -Method Post -Headers $requestHeaders -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($requestBody))
```

请求正文接受 UTF-8 JSON 和单一 `identity` 或 `gzip` 编码。路由精确匹配公开模型名；网关只替换顶层模型字符串，未知普通字段保留。gzip 请求无需修改时保留原压缩字节，替换模型时使用 identity 正文并重建相关头。压缩响应原样传输，语义观察降级。客户端使用配置中的 RelayLeaf ClientKey。

```text
go build ./...
go test ./...
go vet ./...
go test -race ./...
```

race 检查需要受支持的 C 编译器，仅属于开发验证依赖。[CI 工作流](.github/workflows/backend.yml) 从 `backend/go.mod` 读取两段式 Go 版本声明，并通过 `check-latest` 获取该系列的最新补丁版本，配置了 Linux 和 Windows 的后端检查，以及 Linux 的 race 与短时 fuzz。原字节夹具通过 `.gitattributes` 禁止换行转换。

后端源码位于 [backend/](backend/)，可运行配置样例位于 [examples/](examples/)。
