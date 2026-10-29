# 本地中继测试样本

`testdata/responses/` 下的所有文件均为合成样本，使用合成 ID，不含用户凭据，并特意保存为原始字节。它们用于特定的本地断言，**不能**证明真实上游采用这些事件结构，也不能证明完整客户端流程已获支持。`manifest.json` 记录每个文件的 HTTP 元数据、精确大小、SHA-256、传输结束方式及预期事实。更新样本时必须审查这些事实并更新摘要。

`ReadSample(name)` 返回独立的字节副本。`Samples()` 返回独立的清单。测试协议保真度时，不要反序列化后重建样本。JSON 数字的写法、转义后的工具参数、字面量及转义形式的 Unicode、未知字段、资源 ID 和上游模型名称均有意保留。gzip 样本使用 Go 默认的 gzip writer，时间戳为零，OS=255；测试比较完整的压缩字节序列及解码后的源数据。这些样本可在固定的工具链下复现，但不保证未来工具链会产生相同的压缩字节。

`completed-crlf.sse` 是 `completed-lf.sse` 的精确 CRLF 形式。对 `multiline-heartbeat.sse` 进行语义观察时，需要用 LF 拼接 data 行；复制时则保留原始协议行。测试覆盖所有切分位置，包括多字节 Unicode、转义序列和分隔符内部。`truncated.sse` 在 UTF-8 字符及 JSON 内部结束，缺少最终事件分隔符。`terminal-tail.sse` 在终态证据之后仍包含字节，复制器必须继续转发。`terminal-tail-error.sse` 仅提供字节前缀：在其后附加非 EOF 的 `ReadStep.Err`，以模拟尾部 I/O 错误。HTTP EOF/错误属于外部传输事实，无法编码进正文样本。

## 控制 API

- `NewGate().Wait(ctx)` 先标记 `Reached()`，再等待 `Open()`；打开操作幂等。等待被取消时仍记录到达。应通过这些信号同步，避免用休眠同步。
- `NewClock(start)` 提供 `Now`、一次性 `NewTimer` 及显式向前推进的 `Advance`；定时器的 `C()` 带缓冲，`Stop` 幂等。此时钟不控制操作系统 socket I/O 的截止时间。
- `NewScriptedReader(ctx, ReadStep{Bytes, Gate, Err}, ...)` 先返回各段字节，再返回对应错误。它复制输入，在每一步之前暂停，并支持暂停时取消 context。每个 reader 由单个 goroutine 读取。
- `HTTPCount.Wrap(handler)` 和 `ConnState` 在普通 `httptest` 服务器上统计已解析的请求及已接受的连接。正常 H2 传输测试使用 `httptest.NewUnstartedServer`，设置 `Config.ConnState`，并在该服务器上启用 HTTP/2。

## HTTP/2 故障端点

`NewH2Server(H2Options)` 接受使用 ALPN h2 的真实回环 TCP/TLS 连接，并通过 `TLSConfig()` 信任自身的 `httptest` 证书。它读取客户端连接前言及 SETTINGS，发送并确认 SETTINGS 和 PING，观察 HEADERS/DATA，始终不发送 WINDOW_UPDATE。它是小型协议故障注入器，不实现普通 HTTP 服务器，不解码 HPACK，也不校验所有协议规则。

`OnHeaders(ctx, conn, frame)` 针对每个 HEADERS 帧独立执行，同时读取循环继续观察 socket 取消及后续请求。脚本可通过 `Gate` 暂停，调用 `ResetStream`（PROTOCOL_ERROR/REFUSED_STREAM）、`SendGoAway` 或 `WriteFrame`。`conn.SettingsAcknowledged()` 证明客户端已处理初始 SETTINGS。将 `InitialWindowSize` 设置为指向零值的指针，预热并在 ACK 后完成一个请求，再通过同一连接发送正文，以测试流量控制导致的写入阻塞。客户端处理 SETTINGS 前发送的请求可能已有 DATA 在途。

`Counts()` 统计已接受的 TCP 连接及实际 HEADERS 帧，包括可能存在的 trailer。当前请求样本没有 trailer，因此 HEADERS 数即为协议层请求数。`DataBytes()` 统计 DATA 帧的载荷字节数（若带填充，也包含填充）；当前测试使用不带填充的 DATA。仅统计外层 `RoundTrip` 调用次数并不充分：标准 Go Transport 的阳性对照表明，REFUSED_STREAM 和 PROTOCOL_ERROR 都会产生两个 HEADERS。直接调用 `NewClientConn.RoundTrip` 的故障测试则产生一个连接及一个 HEADERS。

`conn.Done()` 记录连接结束，不记录单个流通过 RST_STREAM 取消。连接或服务器关闭时，脚本 context 结束。每个脚本都必须响应该 context，或能独立解除阻塞。`Close()` 停止监听器、取消脚本、关闭 socket，并等待所有样本 goroutine 结束。`Errors()` 报告初始化、脚本及写入错误；预期的对端 EOF/重置属于正常清理。握手、连接前言及写入的安全截止时间为五秒；每个网络测试还应设置较短的 context 截止时间。
