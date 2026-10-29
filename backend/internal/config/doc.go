// Package config 读取、校验并编译文件配置。
//
// LoadFile(ctx, path, Options{ResolveSecrets: bool}) 返回完整的
// *catalog.Snapshot 或错误；调用方不会收到部分编译的配置图。
// Snapshot.GatewayAddress 返回数据监听地址。Snapshot.Summary
// 返回计数（Providers、Targets、Routes、ClientKeys）和 SecretsResolved。
// 文件模式支持直接填写 secret 或通过 secret_ref 引用环境变量、文件，两者互斥。
// 普通检查校验直接填写的材料；ResolveSecrets 同时解析启用组合所需引用并准备运行材料。
// Example 返回文件预设模板，默认使用环境变量引用。
package config
