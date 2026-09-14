# MoviePilot Plugins

MoviePilot 第三方插件仓库，包含兼容维护的 V2 插件和可供 MoviePilot V3 使用的插件实现。

## V3 插件

`package.v3.json` 提供以下 V3 插件索引：

- `Dyg55Indexer`：电影港索引
- `LoumeIndexer`：BT之家索引
- `BtbtlaIndexer`：BT影视索引
- `LdysgIndexer`：老电影资源索引
- `GyingIndexer`：观影索引
- `XunleiHijackDownloader`：迅雷下载接管

在 MoviePilot V3 中将本仓库地址添加为插件市场仓库即可安装。V3 源码位于
`plugins.v3/`，并要求宿主版本 `>=3.0.0`。老电影资源的 OCR 依赖由其
`pyproject.toml` 声明，不会在插件运行时安装依赖。

## V2 兼容

V2 实现和索引继续保留在 `plugins.v2/` 与 `package.v2.json` 中，所有条目均标记
`"v3": false`，避免 V3 运行时误选旧实现。
