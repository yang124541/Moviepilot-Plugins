# MoviePilot GYing Indexer Plugin

为 MoviePilot 增加 gying 站点搜索支持，并抓取磁力链接。

## 功能
- 自动注册 gying 内建索引器（无需 customindexer 再手动填 JSON）
- 接管 `search_torrents` / `async_search_torrents`
- 两段抓取：搜索页取资源 ID，详情页提取 magnet
- 默认仅保留“中字1080P / 中字4K”

## 安装
1. 将 `gyingindexer` 目录放入 MoviePilot 的 `app/plugins/`。
2. 重启 MoviePilot。
3. 在插件页面启用 `GYing Indexer`。
4. 在站点管理新增 `https://www.gying.si/`，填写 Cookie 和 User-Agent。

## 配置项
- `enabled`：启用插件
- `strict_quality`：仅保留中字1080P/中字4K（默认开启）
- `extra_hosts`：额外域名（每行一个）

## 注意
- 该插件依赖登录态 Cookie。
- 建议将账户密码改为新密码并妥善保管。
