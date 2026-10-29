package testkit

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSamplesManifestMatchesExactWireBytes(t *testing.T) {
	m, err := Samples()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Provenance, "合成样本") {
		t.Fatal("缺少合成样本来源说明")
	}
	if len(m.Samples) < 25 {
		t.Fatal("样本覆盖不足")
	}
	seen := make(map[string]bool)
	for _, s := range m.Samples {
		t.Run(s.Name, func(t *testing.T) {
			if seen[s.Name] {
				t.Fatal("清单中的样本重复")
			}
			seen[s.Name] = true
			raw, err := ReadSample(s.Name)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(raw)
			if len(raw) != s.Bytes || hex.EncodeToString(hash[:]) != s.SHA256 {
				t.Fatalf("原始样本已改变；请审查后更新清单中的预期事实及摘要")
			}
			if len(s.Facts) == 0 {
				t.Fatal("样本缺少预期事实")
			}
			if strings.HasSuffix(s.Name, ".json") && !json.Valid(raw) {
				t.Fatal("完整 JSON 样本无效")
			}
			if len(raw) > 0 {
				raw[0] ^= 0xff
				again, err := ReadSample(s.Name)
				if err != nil {
					t.Fatal(err)
				}
				if raw[0] == again[0] {
					t.Fatal("ReadSample 泄露了可变存储")
				}
			}
		})
	}
	files, err := sampleFiles.ReadDir("testdata/responses")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Name() != "manifest.json" && !seen[f.Name()] {
			t.Fatalf("未登记的样本 %s", f.Name())
		}
	}
	for _, bad := range []string{"../completed.json", "a/b", "a\\b", ""} {
		if _, err := ReadSample(bad); err == nil {
			t.Fatalf("错误接受了样本路径 %q", bad)
		}
	}
}

func TestGzipStoredBytesAreReproducible(t *testing.T) {
	m, err := Samples()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range m.Samples {
		if s.GzipSource != "" {
			t.Run(s.Name, func(t *testing.T) {
				raw, err := ReadSample(s.GzipSource)
				if err != nil {
					t.Fatal(err)
				}
				stored, err := ReadSample(s.Name)
				if err != nil {
					t.Fatal(err)
				}
				var encoded bytes.Buffer
				w := gzip.NewWriter(&encoded)
				if _, err := w.Write(raw); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(encoded.Bytes(), stored) {
					t.Fatal("gzip 头部或压缩字节已改变")
				}
				r, err := gzip.NewReader(bytes.NewReader(stored))
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := io.ReadAll(r)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(decoded, raw) {
					t.Fatal("gzip 改变了源字节")
				}
			})
		}
	}
	bad, err := ReadSample("truncated-gzip.bin")
	if err != nil {
		t.Fatal(err)
	}
	r, err := gzip.NewReader(bytes.NewReader(bad))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("截断的 gzip 被视为完整数据")
	}
}

func TestSSEFixturesFramingAndArbitraryChunkBoundaries(t *testing.T) {
	m, err := Samples()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range m.Samples {
		if strings.HasSuffix(s.Name, ".sse") {
			t.Run(s.Name, func(t *testing.T) {
				raw, err := ReadSample(s.Name)
				if err != nil {
					t.Fatal(err)
				}
				// 遍历所有两段切分位置，覆盖分隔符、JSON 转义和 UTF-8 内部。
				for split := 0; split <= len(raw); split++ {
					r := NewScriptedReader(context.Background(), ReadStep{Bytes: raw[:split]}, ReadStep{Bytes: raw[split:]})
					got, err := io.ReadAll(r)
					if err != nil || !bytes.Equal(got, raw) {
						t.Fatalf("在切分位置 %d 处协议字节被改变：%v", split, err)
					}
				}
				if s.Name == "truncated.sse" {
					if utf8.Valid(raw) {
						t.Fatal("截断应位于 UTF-8 字符内部")
					}
					return
				}
				lf := bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
				if !bytes.HasSuffix(lf, []byte("\n\n")) {
					t.Fatal("完整样本缺少最终 SSE 分隔符")
				}
				for _, event := range bytes.Split(lf, []byte("\n\n")) {
					var lines [][]byte
					for _, line := range bytes.Split(event, []byte("\n")) {
						if bytes.HasPrefix(line, []byte("data:")) {
							data := bytes.TrimPrefix(line, []byte("data:"))
							data = bytes.TrimPrefix(data, []byte(" "))
							lines = append(lines, data)
						}
					}
					if len(lines) > 0 && !json.Valid(bytes.Join(lines, []byte("\n"))) {
						t.Fatalf("无效的事件数据：%s", event)
					}
				}
			})
		}
	}
	lf, _ := ReadSample("completed-lf.sse")
	crlf, _ := ReadSample("completed-crlf.sse")
	if !bytes.Equal(bytes.ReplaceAll(lf, []byte("\n"), []byte("\r\n")), crlf) {
		t.Fatal("CRLF 样本的语义不一致")
	}
}

func TestRequestOpaqueSpellingIsIntentional(t *testing.T) {
	raw, err := ReadSample("request-rich.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{`"model" : "public-模型"`, `9007199254740993`, `18446744073709551615`, `1.2300e+12`, `\u4e2d`, `你好 🌿 é`, `unknown_vendor_field`, `"const":"do-not-rewrite"`} {
		if !bytes.Contains(raw, []byte(literal)) {
			t.Fatalf("缺少保真样本内容 %q", literal)
		}
	}
	tool, err := ReadSample("request-tool-arguments.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(tool, []byte(`\"integer\":9007199254740993`)) {
		t.Fatal("缺少不透明参数中的数字")
	}
}
