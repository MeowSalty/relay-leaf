# RelayLeaf

本地即用、可自托管的轻量 AI 网关：按协议、模型和权限选择上游，保留原协议转发，并提供调度、健康管理和可观测性。

已提供配置模板生成、离线检查、秘密解析和本地启动。数据端口统一返回 HTTP 503，尚未提供请求转发。传输与原字节样本的测试范围见 [测试样本说明](backend/internal/testkit/README.md)。

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
client_auth:
  keys:
    local-client:
      secret: "rlk_example_0123456789abcdefghijklmnopqrstuv"
      secret_version: v1
      allowed_routes: [example-responses]
providers:
  example-provider:
    credentials:
      example-key:
        secret: "sk-example-upstream-key"
        secret_version: v1
```

上例密钥是虚构示例，使用时替换为自己的材料，并删除同一对象原有的 `secret_ref`。轮换中的 `retiring_secret` 也支持相同的来源选择。`secret` 必须是非空 YAML 字符串，建议加引号；值不做环境变量替换、不解析 `env:`/`file:` 前缀，也不修剪空白。ClientKey 必须为 32–512 字节的 ASCII Bearer token，实际使用时应采用足够随机的材料；上游密钥须满足端点的认证格式。更换密钥材料时同步更新 `secret_version`。上游 origin 和模型名是示例，按实际服务修改。配置检查和启动检查不会请求上游。

普通 `config check` 校验已填写的 `secret` 及来源字段，不读取环境变量或密钥文件；`--resolve-secrets` 和启动检查按启用状态与有效期解析全部必需材料，并检查客户端密钥是否重复。两种检查的输出均不包含秘密值。`file:` 引用相对配置文件目录解析。无参数启动使用当前目录的 `relayleaf.yaml`；缺文件或端口占用明确失败，不切换配置来源，也不递增端口。默认数据监听为 `127.0.0.1:17890`，没有管理监听。正常停止退出码为 0，配置或启动错误为 1，参数错误为 2。

[最小文件样例](examples/relayleaf.file.yaml) 由同一模板生成。样例显式关闭自动重试、自动熔断和 UI，空回检测保留配置默认值。配置检查通过表示字段和引用关系有效；数据端口尚未执行路由、鉴权准入或转发策略。

```text
go build ./...
go test ./...
go vet ./...
go test -race ./...
```

race 检查需要受支持的 C 编译器，仅属于开发验证依赖。[CI 工作流](.github/workflows/backend.yml) 从 `backend/go.mod` 读取两段式 Go 版本声明，并通过 `check-latest` 获取该系列的最新补丁版本，配置了 Linux 和 Windows 的后端检查，以及 Linux 的 race 与短时 fuzz。原字节夹具通过 `.gitattributes` 禁止换行转换。

后端源码位于 [backend/](backend/)，可运行配置样例位于 [examples/](examples/)。
