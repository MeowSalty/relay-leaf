package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/config"
)

func command(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errs bytes.Buffer
	code := Run(context.Background(), args, &out, &errs)
	return code, out.String(), errs.String()
}

func TestInitDoesNotOverwriteAndReferencesOnlyExternalSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relayleaf.yaml")
	code, out, errs := command(t, "init", "--preset", "file", "--output", path)
	if code != 0 {
		t.Fatalf("init 返回 %d：%s", code, errs)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, config.Example()) || !strings.Contains(out, "RELAYLEAF_CLIENT_KEY") {
		t.Fatal("init 未生成约定的模板和秘密设置说明")
	}
	if code, _, errs := command(t, "init", "--output", path); code != 1 || !strings.Contains(errs, "已存在") {
		t.Fatalf("init 尝试覆盖文件时返回 %d：%s", code, errs)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, after) {
		t.Fatal("已有配置被修改")
	}
	if files, err := os.ReadDir(filepath.Dir(path)); err != nil || len(files) != 1 {
		t.Fatal("init 创建了配置模板以外的文件")
	}
}

func TestCLIArgumentsAndHelp(t *testing.T) {
	secret := "this-accidentally-supplied-secret-must-not-be-echoed"
	for _, args := range [][]string{
		{secret}, {"init", "--preset", "database"}, {"init", "--output", ""},
		{"init", "--unknown=" + secret}, {"local", "--source", "database"},
		{"config"}, {"config", "reload"}, {"config", "check", secret},
		{"config", "check", "--resolve-secrets=" + secret}, {"version", secret},
	} {
		code, out, errs := command(t, args...)
		if code != 2 {
			t.Errorf("参数组合 %q 返回 %d，预期为 2", args[0], code)
		}
		if strings.Contains(out+errs, secret) {
			t.Fatal("参数错误信息泄露了参数值")
		}
	}
	for _, args := range [][]string{{"--help"}, {"help"}, {"local", "--help"}, {"config", "check", "--help"}} {
		code, out, errs := command(t, args...)
		if code != 0 || !strings.Contains(out, "POST /v1/responses") || errs != "" {
			t.Fatalf("帮助命令返回 %d：%s %s", code, out, errs)
		}
	}
	code, out, errs := command(t, "version")
	if code != 0 || !strings.HasPrefix(out, "RelayLeaf "+Version+" (") || !strings.Contains(out, "revision=") || errs != "" {
		t.Fatalf("version 返回 %d：%s %s", code, out, errs)
	}
}

func TestConfigCommandsAreOfflineAndResolveOnlyOnRequest(t *testing.T) {
	var sent atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	path := writeConfig(t, strings.ReplaceAll(string(config.Example()), "https://api.example.com", upstream.URL))
	t.Setenv("RELAYLEAF_CLIENT_KEY", "")
	t.Setenv("RELAYLEAF_UPSTREAM_KEY", "")
	code, out, errs := command(t, "config", "check", "--config", path)
	if code != 0 || !strings.Contains(out, "secrets_resolved=false") {
		t.Fatalf("普通检查读取了不可用的秘密：%d %s", code, errs)
	}
	if code, _, _ := command(t, "config", "check", "--config", path, "--resolve-secrets"); code != 1 {
		t.Fatalf("缺少必需材料时返回 %d", code)
	}
	clientSecret := strings.Repeat("c", 32)
	upstreamSecret := "test-upstream-material-not-for-output"
	t.Setenv("RELAYLEAF_CLIENT_KEY", clientSecret)
	t.Setenv("RELAYLEAF_UPSTREAM_KEY", upstreamSecret)
	code, out, errs = command(t, "config", "check", "--config", path, "--resolve-secrets")
	if code != 0 || !strings.Contains(out, "secrets_resolved=true") {
		t.Fatalf("秘密解析检查失败：%d %s", code, errs)
	}
	if strings.Contains(out+errs, clientSecret) || strings.Contains(out+errs, upstreamSecret) {
		t.Fatal("配置输出泄露了秘密材料")
	}
	if sent.Load() != 0 {
		t.Fatal("离线校验向上游发起了请求")
	}
}

func TestMissingConfigurationDoesNotChooseAnotherSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	code, _, errs := command(t, "local", "--source", "file", "--config", path)
	if code != 1 || !strings.Contains(errs, "relayleaf init") {
		t.Fatalf("缺少配置文件时启动返回 %d：%s", code, errs)
	}
	if entries, err := os.ReadDir(filepath.Dir(path)); err != nil || len(entries) != 0 {
		t.Fatal("缺少配置时创建了备用存储")
	}
}

func TestDefaultInvocationAndCheckedInExample(t *testing.T) {
	checkedIn, err := os.ReadFile(filepath.Join("..", "..", "..", "examples", "relayleaf.file.yaml"))
	if err != nil || !bytes.Equal(checkedIn, config.Example()) {
		t.Fatal("可运行示例必须由 CLI 模板生成")
	}
	t.Chdir(t.TempDir())
	if code, _, errs := command(t); code != 1 || !strings.Contains(errs, "relayleaf init") {
		t.Fatalf("默认配置文件缺失时，无参数调用返回 %d：%s", code, errs)
	}
	if code, _, errs := command(t, "init"); code != 0 {
		t.Fatalf("默认 init 返回 %d：%s", code, errs)
	}
	if code, _, errs := command(t, "config", "check"); code != 0 {
		t.Fatalf("使用默认配置路径时返回 %d：%s", code, errs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := Run(ctx, nil, io.Discard, io.Discard); code != 0 {
		t.Fatalf("正常取消启动时返回 %d", code)
	}
}

func TestConfigurationFailurePrecedesListening(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	data := strings.ReplaceAll(string(config.Example()), "127.0.0.1:17890", occupied.Addr().String())
	data = strings.Replace(data, "version: 1", "version: 99", 1)
	code, _, errs := command(t, "local", "--config", writeConfig(t, data))
	if code != 1 || !strings.Contains(errs, "version") || strings.Contains(errs, "绑定失败") {
		t.Fatalf("无效配置进入了监听启动流程：%d %s", code, errs)
	}
}

func TestValidConfigurationDoesNotMoveAnOccupiedPort(t *testing.T) {
	t.Setenv("RELAYLEAF_CLIENT_KEY", strings.Repeat("c", 32))
	t.Setenv("RELAYLEAF_UPSTREAM_KEY", "test-upstream-key")
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	data := strings.ReplaceAll(string(config.Example()), "127.0.0.1:17890", occupied.Addr().String())
	code, _, errs := command(t, "local", "--config", writeConfig(t, data))
	if code != 1 || !strings.Contains(errs, "绑定失败") {
		t.Fatalf("端口被占用时启动返回 %d：%s", code, errs)
	}
}

type startupOutput struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	started chan string
}

func (o *startupOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.buf.Write(p)
	var record struct {
		Address string `json:"address"`
	}
	if json.Unmarshal(p, &record) == nil && record.Address != "" {
		select {
		case o.started <- record.Address:
		default:
		}
	}
	return n, err
}

func (o *startupOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

func TestListenerForwardsDataAndStops(t *testing.T) {
	const upstreamBody = `{"id":"resp_smoke","object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"成功"}]}]}`
	var sent atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer smoke-upstream" || !bytes.Contains(body, []byte(`"example-upstream-model"`)) {
			t.Error("启动后的真实上游请求不符合配置")
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, upstreamBody)
	}))
	t.Cleanup(upstream.Close)
	data := strings.ReplaceAll(string(config.Example()), "https://api.example.com", upstream.URL)
	data = strings.ReplaceAll(data, "127.0.0.1:17890", "127.0.0.1:17891")
	snapshot, err := config.LoadFile(context.Background(), writeConfig(t, data), config.Options{ResolveSecrets: true, LookupEnv: func(name string) (string, bool) {
		if name == "RELAYLEAF_CLIENT_KEY" {
			return strings.Repeat("c", 32), true
		}
		if name == "RELAYLEAF_UPSTREAM_KEY" {
			return "smoke-upstream", true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 直接构造监听地址为临时端口的快照，避免测试争用固定端口。
	snapshot = catalog.NewSnapshot(catalog.Data{Gateway: "127.0.0.1:0", AccessGroups: snapshot.AccessGroups(), Providers: snapshot.Providers(), Targets: snapshot.Targets(), Routes: snapshot.Routes(), ClientKeys: snapshot.ClientKeys(), Verifications: []catalog.Verification{{ClientID: "local-client", Material: catalog.NewSecret(strings.Repeat("c", 32))}}, Limits: snapshot.Limits(), Execution: snapshot.Execution(), Health: snapshot.Health(), Observability: snapshot.Observability(), SecretsResolved: true})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	output := &startupOutput{started: make(chan string, 1)}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, snapshot, output) }()
	var address string
	select {
	case address = <-output.started:
	case err := <-done:
		t.Fatalf("服务器在开始监听前已退出：%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("监听未启动")
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/v1/responses", "/unknown", "/readyz", "/admin/status"} {
		req, err := http.NewRequest(http.MethodPost, "http://"+address+path, strings.NewReader(`{"model":"example-assistant","input":"测试","stream":false}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("c", 32))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("读取数据响应失败：%v %v", readErr, closeErr)
		}
		if path == "/v1/responses" {
			if resp.StatusCode != 200 || string(body) != upstreamBody {
				t.Fatalf("真实数据转发失败：%d %s", resp.StatusCode, body)
			}
		} else if resp.StatusCode != 404 {
			t.Fatalf("未知路径 %s 返回 %d", path, resp.StatusCode)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("监听未停止")
	}
	if !strings.Contains(output.String(), `"ready":true`) || !strings.Contains(output.String(), `"ready":false`) || !strings.Contains(output.String(), `"config_revision":1`) || sent.Load() != 1 {
		t.Fatal("启动和停机就绪状态、配置身份或实际尝试次数不正确")
	}
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("进程停止后数据监听仍处于打开状态")
	}
}

func writeConfig(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "relayleaf.yaml")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
