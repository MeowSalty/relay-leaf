package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"relayleaf/internal/catalog"
)

const MaxFileBytes = 4 << 20
const MaxDepth = 64
const MaxNodes = 100000
const MaxSecretBytes = 64 << 10

type Options struct {
	ResolveSecrets bool
	LookupEnv      func(string) (string, bool)
	Now            func() time.Time
}
type Error struct {
	File, Path, Message string
	Line, Column        int
	cause               error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s: %s", e.File, e.Line, e.Column, e.Path, e.Message)
}
func (e *Error) Unwrap() error { return e.cause }

type compiler struct {
	file    string
	nodes   map[string]*yaml.Node
	options Options
	now     time.Time
}

func (c *compiler) err(path, message string) error {
	n := c.nodes[path]
	line, col := 1, 1
	if n != nil {
		line, col = n.Line, n.Column
	}
	return &Error{File: c.file, Path: path, Message: message, Line: line, Column: col}
}
func (c *compiler) errAt(path, message string, n *yaml.Node) error {
	return &Error{File: c.file, Path: path, Message: message, Line: n.Line, Column: n.Column}
}
func keyPath(parent, key string) string {
	if safePathPart(key) {
		return parent + "." + key
	}
	return parent + ".[key]"
}
func LoadFile(ctx context.Context, path string, options Options) (*catalog.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, &Error{File: path, Path: "$", Message: "无法打开配置文件", Line: 1, Column: 1, cause: err}
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxFileBytes+1))
	if err != nil {
		return nil, &Error{File: path, Path: "$", Message: "无法读取配置文件", Line: 1, Column: 1, cause: err}
	}
	return loadBytes(ctx, path, data, options)
}
func loadBytes(ctx context.Context, path string, data []byte, options Options) (*catalog.Snapshot, error) {
	c := compiler{file: path, nodes: map[string]*yaml.Node{}, options: options, now: time.Now()}
	if options.Now != nil {
		c.now = options.Now()
	}
	if c.options.LookupEnv == nil {
		c.options.LookupEnv = os.LookupEnv
	}
	if len(data) > MaxFileBytes {
		return nil, c.err("$", "配置文件超过 4 MiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		return nil, c.syntaxError(err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, c.err("$", "仅允许一个 YAML 文档")
	}
	if len(root.Content) != 1 || root.Content[0].Kind != yaml.MappingNode {
		return nil, c.err("$", "配置必须为映射")
	}
	count := 0
	if err := c.checkNodes(root.Content[0], "$", 1, &count); err != nil {
		return nil, err
	}
	if err := c.checkSchema(root.Content[0], reflect.TypeFor[document](), "$"); err != nil {
		return nil, err
	}
	var dto document
	strict := yaml.NewDecoder(bytes.NewReader(data))
	strict.KnownFields(true)
	if err := strict.Decode(&dto); err != nil {
		return nil, c.syntaxError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.compile(ctx, dto)
}

var yamlLine = regexp.MustCompile(`line (\d+)`)

func (c *compiler) syntaxError(err error) error {
	e := c.err("$", "YAML 语法或字段类型无效").(*Error)
	if m := yamlLine.FindStringSubmatch(err.Error()); len(m) == 2 {
		e.Line, _ = strconv.Atoi(m[1])
	}
	return e
}
func (c *compiler) checkNodes(n *yaml.Node, path string, depth int, count *int) error {
	*count++
	c.nodes[path] = n
	if depth > MaxDepth {
		return c.err(path, "YAML 嵌套超过 64 层")
	}
	if *count > MaxNodes {
		return c.err(path, "YAML 节点数超过 100000")
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return c.err(path, "不允许使用 YAML 锚点和别名")
	}
	switch n.Kind {
	case yaml.MappingNode:
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			*count++
			if *count > MaxNodes {
				return c.err(path, "YAML 节点数超过 100000")
			}
			if k.Anchor != "" {
				return c.errAt(keyPath(path, k.Value), "不允许使用 YAML 锚点和别名", k)
			}
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || k.Value == "<<" {
				return c.errAt(keyPath(path, k.Value), "映射键必须为字符串；不允许使用合并键", k)
			}
			if seen[k.Value] {
				return c.errAt(keyPath(path, k.Value), "映射键重复", k)
			}
			seen[k.Value] = true
			child := path + "." + k.Value
			if !safePathPart(k.Value) {
				child = path + ".[key]"
			}
			if err := c.checkNodes(n.Content[i+1], child, depth+1, count); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for i, child := range n.Content {
			if err := c.checkNodes(child, fmt.Sprintf("%s[%d]", path, i), depth+1, count); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if n.Tag != "!!str" && n.Tag != "!!int" && n.Tag != "!!float" && n.Tag != "!!bool" && n.Tag != "!!null" && n.Tag != "!!timestamp" {
			return c.err(path, "不支持的 YAML 标量标签")
		}
	default:
		return c.err(path, "不支持的 YAML 节点")
	}
	return nil
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func safePathPart(s string) bool { return idPattern.MatchString(s) }
func (c *compiler) checkSchema(n *yaml.Node, t reflect.Type, path string) error {
	return c.checkSchemaAt(n, t, path, false, false, 0)
}
func (c *compiler) checkSchemaAt(n *yaml.Node, t reflect.Type, path string, inExecution, nullable bool, level int) error {
	if t == reflect.TypeFor[*executionDTO]() || t == reflect.TypeFor[executionDTO]() {
		inExecution = true
	}
	if n.Tag == "!!null" {
		if nullable || inExecution && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice) {
			return nil
		}
		return c.err(path, "此处不允许 null")
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return c.err(path, "需要映射")
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := f.Tag.Get("yaml")
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			fields[name] = f.Type
		}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i].Value
			if inExecution && (level > 0 && (k == "max_retries" || k == "downstream_write_idle_timeout" || k == "request_body_read_timeout") || level > 1 && (k == "max_retries_per_provider" || k == "rules")) {
				return c.errAt(keyPath(path, k), "此层级不允许覆盖该字段", n.Content[i])
			}
			ft, ok := fields[k]
			if !ok {
				if k == "proxy" || k == "tls" || k == "transport" {
					return c.errAt(keyPath(path, k), "不支持：尚未实现高级传输设置", n.Content[i])
				}
				return c.errAt(keyPath(path, k), "未知字段", n.Content[i])
			}
			canExpire := t == reflect.TypeFor[clientKeyDTO]() && k == "expires_at"
			if (k == "secret" || k == "secret_ref") && n.Content[i+1].Tag != "!!str" {
				return c.err(path+"."+k, "密钥来源必须为 YAML 字符串")
			}
			nextLevel := level
			if ft == reflect.TypeFor[*executionDTO]() {
				if t == reflect.TypeFor[providerDTO]() {
					nextLevel = 1
				}
				if t == reflect.TypeFor[credentialDTO]() {
					nextLevel = 2
				}
			}
			if err := c.checkSchemaAt(n.Content[i+1], ft, path+"."+k, inExecution, canExpire, nextLevel); err != nil {
				return err
			}
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return c.err(path, "需要映射")
		}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i].Value
			if !safePathPart(k) && t.Elem().Kind() != reflect.String {
				return c.err(path, "稳定标识符无效")
			}
			p := path + "." + k
			if !safePathPart(k) {
				p = path + ".[key]"
			}
			if err := c.checkSchemaAt(n.Content[i+1], t.Elem(), p, inExecution, false, level); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return c.err(path, "需要序列")
		}
		for i, child := range n.Content {
			if err := c.checkSchemaAt(child, t.Elem(), fmt.Sprintf("%s[%d]", path, i), false, false, level); err != nil {
				return err
			}
		}
	case reflect.String:
		if n.Kind != yaml.ScalarNode || (n.Tag != "!!str" && n.Tag != "!!timestamp") {
			return c.err(path, "需要字符串")
		}
	case reflect.Bool:
		if n.Tag != "!!bool" {
			return c.err(path, "需要布尔值")
		}
	case reflect.Int, reflect.Int64:
		if n.Tag != "!!int" {
			return c.err(path, "需要整数")
		}
		var integer int64
		if err := n.Decode(&integer); err != nil {
			return c.err(path, "整数超出可表示范围")
		}
	case reflect.Float64:
		if n.Tag != "!!int" && n.Tag != "!!float" {
			return c.err(path, "需要数值")
		}
	}
	return nil
}
func (c *compiler) resolve(ctx context.Context, secret, ref *string, path string) (catalog.Secret, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Secret{}, err
	}
	if err := c.secretSource(secret, ref, path, true); err != nil {
		return catalog.Secret{}, err
	}
	path = secretPath(secret, path)
	if secret != nil {
		return catalog.NewSecret(*secret), nil
	}
	kind, value, _ := strings.Cut(*ref, ":")
	var material string
	if kind == "env" {
		var found bool
		material, found = c.options.LookupEnv(value)
		if !found {
			return catalog.Secret{}, c.err(path, "密钥环境变量不可用")
		}
	} else {
		secretPath := value
		if !filepath.IsAbs(secretPath) {
			secretPath = filepath.Join(filepath.Dir(c.file), secretPath)
		}
		f, err := os.Open(secretPath)
		if err != nil {
			e := c.err(path, "无法打开密钥文件").(*Error)
			e.cause = err
			return catalog.Secret{}, e
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, MaxSecretBytes+1))
		if err != nil {
			return catalog.Secret{}, c.err(path, "无法读取密钥文件")
		}
		material = string(b)
	}
	if len(material) > MaxSecretBytes {
		return catalog.Secret{}, c.err(path, "密钥内容超过 64 KiB")
	}
	if kind == "file" && strings.HasSuffix(material, "\n") {
		material = strings.TrimSuffix(material, "\n")
		material = strings.TrimSuffix(material, "\r")
	}
	if err := c.validateSecret(material, path); err != nil {
		return catalog.Secret{}, err
	}
	if err := ctx.Err(); err != nil {
		return catalog.Secret{}, err
	}
	return catalog.NewSecret(material), nil
}
func (c *compiler) validateSecret(material, path string) error {
	if len(material) > MaxSecretBytes {
		return c.err(path, "密钥内容超过 64 KiB")
	}
	if material == "" || strings.IndexFunc(material, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
		return c.err(path, "密钥内容为空或包含控制字符")
	}
	return nil
}
