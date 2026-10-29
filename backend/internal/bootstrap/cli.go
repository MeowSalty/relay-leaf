// Package bootstrap 组装命令行入口和文件模式进程。
package bootstrap

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"

	"relayleaf/internal/config"
)

// Version 由发行构建设置，开发构建不代表已发布版本。
var Version = "dev"

const usage = `RelayLeaf — 文件模式网关

用法：
  relayleaf init --preset file [--output relayleaf.yaml]
  relayleaf config check [--config relayleaf.yaml] [--resolve-secrets]
  relayleaf local --source file [--config relayleaf.yaml]
  relayleaf version

无参数启动时，使用本地文件模式并读取 ./relayleaf.yaml。
数据入口支持 POST /v1/responses，使用 RelayLeaf ClientKey 的 Bearer 认证。
`

// Run 在成功或正常停止时返回 0，配置或启动失败时返回 1，
// 命令行参数无效时返回 2。本函数不会退出进程。
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return runLocal(ctx, nil, stdout, stderr)
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "version", "--version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "version 不接受参数")
			return 2
		}
		return printVersion(stdout)
	case "init":
		return runInit(args[1:], stdout, stderr)
	case "config":
		if len(args) < 2 || args[1] != "check" {
			fmt.Fprintln(stderr, "用法：relayleaf config check [--config relayleaf.yaml] [--resolve-secrets]")
			return 2
		}
		return runCheck(ctx, args[2:], stdout, stderr)
	case "local":
		return runLocal(ctx, args[1:], stdout, stderr)
	default:
		// 不回显无法识别的参数，避免将误传的秘密写入日志。
		fmt.Fprintln(stderr, "未知命令；请使用 relayleaf --help 查看帮助")
		return 2
	}
}

func parseFlags(fs *flag.FlagSet, args []string, stdout, stderr io.Writer) int {
	// flag 的默认诊断会回显无效参数值。对外错误不包含用户传入的值，
	// 避免泄露误传的秘密。
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		fmt.Fprintln(stderr, "命令行参数无效；请使用 relayleaf --help 查看帮助")
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "存在多余的位置参数；请使用 relayleaf --help 查看帮助")
		return 2
	}
	return -1
}

func runInit(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	preset := fs.String("preset", "file", "配置预设")
	output := fs.String("output", "relayleaf.yaml", "新配置文件")
	if code := parseFlags(fs, args, stdout, stderr); code >= 0 {
		return code
	}
	if *preset != "file" || *output == "" {
		fmt.Fprintln(stderr, "init 要求使用 file 预设，且输出路径不能为空")
		return 2
	}
	if err := writeNewFile(*output, config.Example()); err != nil {
		if errors.Is(err, os.ErrExist) {
			fmt.Fprintln(stderr, "配置文件已存在；请通过 --output 指定其他路径")
		} else {
			fmt.Fprintln(stderr, "无法创建配置文件；请检查父目录及其权限")
		}
		return 1
	}
	fmt.Fprintln(stdout, "已创建文件模式配置。请设置 RELAYLEAF_CLIENT_KEY 和 RELAYLEAF_UPSTREAM_KEY，或将对应的 secret_ref 改为 secret 直接填写密钥，然后运行 config check --resolve-secrets。")
	fmt.Fprintln(stdout, "上游 origin 和模型仅为示例。修改后可通过 POST /v1/responses 调用配置的公开模型。")
	return 0
}

func writeNewFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		// 本次调用以独占方式创建了文件，清理时仅删除这个未写完整的文件。
		removeErr := os.Remove(path)
		return errors.Join(writeErr, closeErr, removeErr)
	}
	return nil
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("config check", flag.ContinueOnError)
	path := fs.String("config", "relayleaf.yaml", "配置文件")
	resolve := fs.Bool("resolve-secrets", false, "检查必需的秘密材料")
	if code := parseFlags(fs, args, stdout, stderr); code >= 0 {
		return code
	}
	if *path == "" {
		fmt.Fprintln(stderr, "--config 的路径不能为空")
		return 2
	}
	snapshot, err := config.LoadFile(ctx, *path, config.Options{ResolveSecrets: *resolve})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	summary := snapshot.Summary()
	fmt.Fprintf(stdout, "配置有效：providers=%d targets=%d routes=%d client_keys=%d secrets_resolved=%t\n", summary.Providers, summary.Targets, summary.Routes, summary.ClientKeys, summary.SecretsResolved)
	if !*resolve {
		fmt.Fprintln(stdout, "已校验直接填写的密钥，未读取环境变量或密钥文件。可使用 --resolve-secrets 检查全部必需材料。")
	}
	return 0
}

func runLocal(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("local", flag.ContinueOnError)
	source := fs.String("source", "file", "配置来源")
	path := fs.String("config", "relayleaf.yaml", "配置文件")
	if code := parseFlags(fs, args, stdout, stderr); code >= 0 {
		return code
	}
	if *source != "file" || *path == "" {
		fmt.Fprintln(stderr, "local 当前要求使用 --source file，且配置文件路径不能为空")
		return 2
	}
	snapshot, err := config.LoadFile(ctx, *path, config.Options{ResolveSecrets: true})
	if err != nil {
		if ctx.Err() != nil {
			return 0
		}
		fmt.Fprintln(stderr, err)
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(stderr, "请运行 relayleaf init --preset file 创建配置。")
		}
		return 1
	}
	if err := serve(ctx, snapshot, stderr); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func printVersion(w io.Writer) int {
	revision := "uncommitted"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
		}
	}
	fmt.Fprintf(w, "RelayLeaf %s (%s %s/%s; revision=%s)\n", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH, revision)
	return 0
}
