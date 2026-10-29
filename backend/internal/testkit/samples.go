package testkit

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed testdata/responses/*
var sampleFiles embed.FS

// Sample 描述合成的原始协议字节及受控的传输结束方式。
// Facts 表示样本的设计意图，不能证明上游符合协议。
type Sample struct {
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	Status       int               `json:"status"`
	Headers      map[string]string `json:"headers"`
	Bytes        int               `json:"bytes"`
	SHA256       string            `json:"sha256"`
	GzipSource   string            `json:"gzip_source"`
	TransportEnd string            `json:"transport_end"`
	Facts        []string          `json:"facts"`
}

type SampleManifest struct {
	Provenance string   `json:"provenance"`
	Samples    []Sample `json:"samples"`
}

// Samples 返回全新的清单，其中的 map 和切片均独立。
func Samples() (SampleManifest, error) {
	raw, err := sampleFiles.ReadFile("testdata/responses/manifest.json")
	if err != nil {
		return SampleManifest{}, err
	}
	var manifest SampleManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return SampleManifest{}, fmt.Errorf("读取样本清单：%w", err)
	}
	return manifest, nil
}

// ReadSample 返回所存原始字节的独立副本。它不会解析、
// 规范化换行、修复截断字节或重建请求/响应 DTO。
func ReadSample(name string) ([]byte, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return nil, fmt.Errorf("无效的样本名称 %q", name)
	}
	raw, err := sampleFiles.ReadFile("testdata/responses/" + name)
	if err != nil {
		return nil, fmt.Errorf("读取样本 %q：%w", name, err)
	}
	return append([]byte{}, raw...), nil
}
