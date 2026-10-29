package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	runstate "relayleaf/internal/runtime"
)

func TestHandlerLongStreamKeepsBoundedResources(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("h2=%t", h2), func(t *testing.T) {
			const blocks, eventsPerBlock = 20, 64
			const responseBudget = 256 << 10
			delta := strings.Repeat("流", 256)
			frame := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg-long\",\"output_index\":0,\"content_index\":0,\"delta\":\"" + delta + "\"}\n\n"
			block := []byte(strings.Repeat(frame, eventsPerBlock))
			text := strings.Repeat(delta, blocks*eventsPerBlock)
			tail := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-life\",\"object\":\"response\",\"status\":\"completed\",\"output\":[{\"id\":\"msg-long\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"" + text + "\"}]}]}}\n\n"
			permits := make(chan struct{}, 1)
			var calls atomic.Int64
			upstream := lifecycleUpstream(t, h2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if h2 && r.ProtoMajor != 2 {
					t.Error("长流上游没有协商到 HTTP/2")
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, lifecycleCreatedEvent)
				_ = http.NewResponseController(w).Flush()
				for i := 0; i <= blocks; i++ {
					select {
					case <-r.Context().Done():
						return
					case <-permits:
					}
					if i == blocks {
						_, _ = io.WriteString(w, tail)
					} else if _, err := w.Write(block); err != nil {
						return
					}
					if err := http.NewResponseController(w).Flush(); err != nil {
						return
					}
				}
			}))
			clock := newLifecycleClock()
			snapshot := contractSnapshot(t, upstream.URL, func(d *catalog.Data) {
				d.Limits.MaxTotalResponseBufferBytes = responseBudget
				for id, provider := range d.Providers {
					for key, credential := range provider.Credentials {
						credential.Execution.EmptyResponseEnabled = false
						provider.Credentials[key] = credential
					}
					d.Providers[id] = provider
				}
			})
			h, err := NewHandler(snapshot, HandlerOptions{BaseTransport: upstream.Client().Transport.(*http.Transport), Now: clock.clock.Now, NewTimer: clock.newTimer})
			if err != nil {
				t.Fatal(err)
			}
			gateway, done := lifecycleGatewayProtocol(t, h, h2)
			gateway.Client().Timeout = 20 * time.Second
			request := lifecycleRequest(t, context.Background(), gateway.URL, true)
			request.Header.Set("Authorization", "Bearer "+contractClientSecret)
			started := time.Now()
			response, err := gateway.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatalf("长流未被接纳：%d", response.StatusCode)
			}
			var bodyPeak, responsePeak atomic.Int64
			measure := func(s runstate.Stats) {
				for old := bodyPeak.Load(); s.BodyBytes > old && !bodyPeak.CompareAndSwap(old, s.BodyBytes); old = bodyPeak.Load() {
				}
				for old := responsePeak.Load(); s.ResponseBytes > old && !responsePeak.CompareAndSwap(old, s.ResponseBytes); old = responsePeak.Load() {
				}
			}
			stopMeasure, measured := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(measured)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopMeasure:
						return
					case <-ticker.C:
						measure(h.Stats())
					}
				}
			}()
			defer func() { close(stopMeasure); <-measured }()
			initialEntries := h.Stats().StateEntries
			expected, received := sha256.New(), sha256.New()
			_, _ = io.WriteString(expected, lifecycleCreatedEvent)
			if _, err := io.CopyN(received, response.Body, int64(len(lifecycleCreatedEvent))); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, len(block))
			for i := 0; i < blocks; i++ {
				permits <- struct{}{}
				if _, err := io.ReadFull(response.Body, buffer); err != nil {
					t.Fatalf("第 %d 块流被提前截断：%v", i, err)
				}
				if !bytes.Equal(buffer, block) {
					t.Fatalf("第 %d 块原始字节变化", i)
				}
				_, _ = expected.Write(block)
				_, _ = received.Write(buffer)
				s := h.Stats()
				measure(s)
				if s.Inbound != 1 || s.ActiveAttempts != 1 || s.StateEntries != initialEntries {
					t.Fatalf("长流在途许可或状态数量变化：%+v", s)
				}
				// 每次推进都小于空闲期限，累计时长超过常见的固定总超时。
				clock.clock.Advance(time.Minute)
			}
			permits <- struct{}{}
			_, _ = io.WriteString(expected, tail)
			if _, err := io.Copy(received, response.Body); err != nil {
				t.Fatalf("长流终局未完整交付：%v", err)
			}
			_ = response.Body.Close()
			lifecycleSignal(t, done, "长流执行清理")
			if !bytes.Equal(received.Sum(nil), expected.Sum(nil)) {
				t.Fatal("长流原始字节校验不一致")
			}
			lifecycleEmpty(t, h, false)
			records := h.Recent()
			if calls.Load() != 1 || h.Stats().Attempts != 1 || len(records) != 1 || records[0].Attempt == nil {
				t.Fatalf("长流产生额外尝试或缺少记录：%+v", records)
			}
			attempt := records[0].Attempt
			wantBytes := int64(len(lifecycleCreatedEvent) + blocks*len(block) + len(tail))
			if attempt.UpstreamBytes != wantBytes || attempt.DownstreamBytes != wantBytes || attempt.FinishedAt.Sub(attempt.StartedAt) != blocks*time.Minute {
				t.Fatalf("长流字节或时长记录错误：%+v", attempt)
			}
			// 汇总事件超过观察上限时保留已知非空和真实 EOF，不能伪造完整语义。
			facts := attempt.Response
			if facts.Output != "nonempty" || !facts.EOF || facts.ProtocolComplete || facts.FirstEvent.IsZero() || facts.FirstContent.IsZero() {
				t.Fatalf("超长汇总事件的观察降级不正确：%+v", facts)
			}
			if bodyPeak.Load() > snapshot.Limits().MaxTotalBodyBufferBytes || responsePeak.Load() <= 0 || responsePeak.Load() > responseBudget {
				t.Fatalf("长流突破实际缓冲额度：入站=%d，响应=%d", bodyPeak.Load(), responsePeak.Load())
			}
			t.Logf("真实传输 %d 字节，实际耗时 %s，逻辑时长 %s，采样入站缓冲峰值 %d，响应缓冲峰值 %d，观察覆盖 %s", wantBytes, time.Since(started), blocks*time.Minute, bodyPeak.Load(), responsePeak.Load(), facts.Coverage)
		})
	}
}

func TestHandlerShutdownDrainsOrCancelsActiveStream(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, force := range []bool{false, true} {
			t.Run(fmt.Sprintf("h2=%t/force=%t", h2, force), func(t *testing.T) {
				release, ended := make(chan struct{}), make(chan struct{})
				var calls atomic.Int64
				upstream := lifecycleUpstream(t, h2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(ended)
					calls.Add(1)
					if h2 && r.ProtoMajor != 2 {
						t.Error("停机验证上游没有协商到 HTTP/2")
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, lifecycleCreatedEvent)
					_ = http.NewResponseController(w).Flush()
					select {
					case <-r.Context().Done():
					case <-release:
						_, _ = io.WriteString(w, ": 正常排空\n\n")
					}
				}))
				clock := newLifecycleClock()
				h := lifecycleHandler(t, upstream, clock, nil)
				gateway, done := lifecycleGatewayProtocol(t, h, h2)
				response, err := gateway.Client().Do(lifecycleRequest(t, context.Background(), gateway.URL, true))
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if _, err := io.CopyN(io.Discard, response.Body, int64(len(lifecycleCreatedEvent))); err != nil {
					t.Fatal(err)
				}
				h.StopAccepting()
				rejected, err := gateway.Client().Do(lifecycleRequest(t, context.Background(), gateway.URL, false))
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, rejected.Body)
				_ = rejected.Body.Close()
				lifecycleSignal(t, done, "停机期间新请求拒绝")
				if rejected.StatusCode != 503 || calls.Load() != 1 || h.Stats().ActiveAttempts != 1 {
					t.Fatalf("停止接纳破坏在途请求或允许新尝试：状态=%d，统计=%+v", rejected.StatusCode, h.Stats())
				}
				closed := make(chan struct{})
				if force {
					go func() { _ = h.Close(); close(closed) }()
				} else {
					close(release)
				}
				tail, readErr := io.ReadAll(response.Body)
				if !force && (readErr != nil || string(tail) != ": 正常排空\n\n") {
					t.Fatalf("正常停机没有完整交付在途响应：%q，%v", tail, readErr)
				}
				lifecycleSignal(t, ended, "停机上游 I/O 收尾")
				lifecycleSignal(t, done, "停机在途执行清理")
				if force {
					lifecycleSignal(t, closed, "强制关闭返回")
				} else {
					_ = h.Close()
				}
				lifecycleEmpty(t, h, true)
				if calls.Load() != 1 || h.Stats().Attempts != 1 {
					t.Fatal("停机清理期间重复发送")
				}
			})
		}
	}
}
