package bootstrap

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/config"
	"relayleaf/internal/relay"
	"relayleaf/internal/testkit"
)

// 通过编译后的配置图验证真实的单次传输与响应复制。
// 测试使用专用下游处理器组装配置和传输原语。
func TestCompiledCandidateDrivesSingleAttemptFixtures(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, fixture := range []struct{ name, contentType, encoding string }{
			{"completed.json.gz", "application/json", "gzip"},
			{"terminal-tail.sse", "text/event-stream", ""},
		} {
			t.Run(fmt.Sprintf("h2=%t/%s", h2, fixture.name), func(t *testing.T) {
				wire, err := testkit.ReadSample(fixture.name)
				if err != nil {
					t.Fatal(err)
				}
				var count testkit.HTTPCount
				var observedProtocol atomic.Int32
				upstreamPath := "/v1/responses"
				if fixture.contentType == "text/event-stream" {
					upstreamPath = "/vendor/%72esponses"
				}
				upstream := httptest.NewUnstartedServer(count.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					observedProtocol.Store(int32(r.ProtoMajor))
					if r.Method != http.MethodPost || r.URL.EscapedPath() != upstreamPath || r.Header.Get("Authorization") != "Bearer fixture-upstream-secret" {
						t.Error("编译后的端点或凭据未传到预期上游")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("读取上游请求正文失败：%v", err)
						return
					}
					w.Header().Set("Content-Type", fixture.contentType)
					if fixture.encoding != "" {
						w.Header().Set("Content-Encoding", fixture.encoding)
					}
					if _, err := w.Write(wire); err != nil {
						t.Errorf("写入上游夹具失败：%v", err)
					}
				})))
				upstream.EnableHTTP2 = h2
				upstream.Config.ConnState = count.ConnState
				upstream.StartTLS()
				t.Cleanup(upstream.Close)
				fileConfig := strings.ReplaceAll(string(config.Example()), "https://api.example.com", upstream.URL+"/")
				fileConfig = strings.Replace(fileConfig, "path_template: /v1/responses", "path_template: "+upstreamPath, 1)
				path := writeConfig(t, fileConfig)
				snapshot, err := config.LoadFile(context.Background(), path, config.Options{ResolveSecrets: true, LookupEnv: func(name string) (string, bool) {
					switch name {
					case "RELAYLEAF_CLIENT_KEY":
						return strings.Repeat("c", 32), true
					case "RELAYLEAF_UPSTREAM_KEY":
						return "fixture-upstream-secret", true
					default:
						return "", false
					}
				}})
				if err != nil {
					t.Fatal(err)
				}
				route := snapshot.Routes()["example-responses"]
				target := snapshot.Targets()[route.Targets[0].Target]
				provider := snapshot.Providers()[target.ProviderID]
				endpoint := provider.Endpoints[target.EndpointID]
				credential := provider.Credentials[target.Credentials[0]]
				base := upstream.Client().Transport.(*http.Transport).Clone()
				base.Protocols = new(http.Protocols)
				base.Protocols.SetHTTP1(true)
				base.Protocols.SetHTTP2(h2)
				transport, err := relay.NewTransport(relay.Options{MaxConnections: snapshot.Limits().MaxInflight, BaseTransport: base})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = transport.Close() })
				var started atomic.Int32
				copied := make(chan error, 1)
				downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// 此处根据已编译的上游模型名称构造合成请求。
					body := fmt.Sprintf(`{"model":%q,"input":"fixture","stream":%t}`, target.UpstreamName, fixture.contentType == "text/event-stream")
					req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, provider.Origin+endpoint.PathTemplate, strings.NewReader(body))
					if err != nil {
						t.Error(err)
						return
					}
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+credential.Secret.Reveal())
					attempt, err := transport.Start(r.Context(), req, func() error { started.Add(1); return nil })
					if err != nil {
						t.Errorf("单次尝试失败：%v", err)
						http.Error(w, "夹具执行失败", http.StatusBadGateway)
						return
					}
					copyErr := relay.CopyResponse(r.Context(), w, attempt.Response, credential.Execution.DownstreamWriteIdleTimeout)
					<-attempt.Done()
					copied <- copyErr
				}))
				t.Cleanup(downstream.Close)
				clientTransport := &http.Transport{DisableCompression: true, Proxy: nil}
				t.Cleanup(clientTransport.CloseIdleConnections)
				client := &http.Client{Transport: clientTransport, Timeout: 5 * time.Second}
				for range 2 {
					resp, err := client.Get(downstream.URL)
					if err != nil {
						t.Fatal(err)
					}
					got, readErr := io.ReadAll(resp.Body)
					closeErr := resp.Body.Close()
					if readErr != nil || closeErr != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, wire) {
						t.Fatalf("原字节复制状态码=%d 读取错误=%v 关闭错误=%v 字节一致=%t", resp.StatusCode, readErr, closeErr, bytes.Equal(got, wire))
					}
					if resp.Header.Get("Content-Encoding") != fixture.encoding {
						t.Fatal("响应编码被修改")
					}
					// 固定长度正文收齐时，服务端仍可能在确认 EOF；先等所属 I/O 收尾。
					select {
					case err := <-copied:
						if err != nil {
							t.Fatalf("复制夹具失败：%v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("响应复制与上游尝试未及时收尾")
					}
				}
				wantProtocol := int32(1)
				if h2 {
					wantProtocol = 2
				}
				if observedProtocol.Load() != wantProtocol || started.Load() != 2 {
					t.Fatalf("协议版本=%d 启动次数=%d", observedProtocol.Load(), started.Load())
				}
				if connections, requests := count.Counts(); connections != 1 || requests != 2 {
					t.Fatalf("配置的传输连接数=%d 请求数=%d；预期复用 1 条连接发送 2 次请求", connections, requests)
				}
				if err := transport.Close(); err != nil {
					t.Fatal(err)
				}
				if stats := transport.Stats(); stats.Connections != 0 || stats.Busy != 0 || stats.ActiveIO != 0 {
					t.Fatalf("配置的传输仍保留资源：%+v", stats)
				}
			})
		}
	}
}
