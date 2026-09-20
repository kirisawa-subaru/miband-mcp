# Gadgetbridge MCP 后端

同一 `miband-health-mcp` 入口支持 `xiaomi_health` 与 `gadgetbridge` 两种后端。Gadgetbridge 直接读取其导出的 SQLite，不需要 root、配对密钥或 Frida 来查询、同步数据库。当前实现面向 Linux/macOS；Windows 尚未完成适配与验收。

手机的安装、配对、权限与导出位置设置由仓库组件 `skills/miband-setup/SKILL.md` 引导。这里只描述安装 agent 需要配置的电脑入口，普通用户不必编辑下面的文件。

## 配置

安装依赖：

```bash
uv sync --locked
```

使用小米健康进程内控制时，安装可选依赖：`uv sync --locked --extra xiaomi`。

为 MCP 客户端准备私有 JSON 文件，例如：

```json
{
  "backend": "gadgetbridge",
  "timezone": "Asia/Shanghai",
  "adb_serial": "YOUR_PHONE_SERIAL",
  "gadgetbridge_remote_db": "/storage/emulated/0/Download/Gadgetbridge.db",
  "gadgetbridge_device_address": "AA:BB:CC:DD:EE:FF"
}
```

手机序列号、手环地址与导出位置由安装 agent 从实际设备/数据库读取，不能照抄示例。多个手环时须明确选择，不能汇总成同一人的数据。ADB 可以显式用 `adb_path` 指定可执行文件。

Gadgetbridge 默认缓存为 `~/.local/share/miband-gadgetbridge/Gadgetbridge.db`；可用 `data_dir` 与 `gadgetbridge_db_path` 覆盖。配置中的相对路径以配置文件目录为起点。文件只读场景可只设置后端与本地数据库路径，不配置手机传输；此时使用 `freshness="cached"`，调用同步会报告缺少传输配置。

客户端通过环境变量明确选择配置，不会自动读取其他客户端的文件：

```json
{
  "mcpServers": {
    "miband-health": {
      "command": "/path/to/MiBand-connection/.venv/bin/miband-health-mcp",
      "env": {
        "MIBAND_HEALTH_CONFIG": "/path/to/private/gadgetbridge.json"
      }
    }
  }
}
```

也可以用 `MIBAND_HEALTH_BACKEND`、`MIBAND_HEALTH_DATA_DIR`、`MIBAND_HEALTH_TIMEZONE`、`MIBAND_HEALTH_ADB_PATH`、`MIBAND_HEALTH_ADB_SERIAL`、`MIBAND_HEALTH_GADGETBRIDGE_DB`、`MIBAND_HEALTH_GADGETBRIDGE_REMOTE_DB`、`MIBAND_HEALTH_GADGETBRIDGE_DEVICE_ID` 或 `MIBAND_HEALTH_GADGETBRIDGE_DEVICE_ADDRESS` 覆盖相应字段。环境变量优先于 JSON。小米健康仍为未配置时的兼容默认后端，两种后端默认使用不同缓存目录。

## 工具与数据语义

Gadgetbridge 模式注册四个工具：

- `get_current_state`：最近心率、近 30 分钟步数、导出库中的电量、最近睡眠和运动记录。心率按记录年龄判断新鲜度，过期时保留 `latest_value`，当前 `value` 为 null。电量是历史观测，不能用于推断当前连接状态。
- `get_daily_report`：指定时区的自然日汇总及历史比较；睡眠归属于醒来日。日汇总与分钟样本不会相加重复统计，缺失返回 null。
- `query_health`：有界、分页的时间序列、睡眠与运动记录。公共时间序列指标为 `heart_rate.bpm`、`steps`、`distance`、`calories`；返回来源与单位。运动仅提供已解析的基础字段，不猜测复杂摘要里的数值。
- `sync_health`：默认请求数据库导出并拉取；`refresh_app=true` 先请求手环历史记录同步。该参数保留旧接口名，不会操作手机界面或主动测一次心率。

`health://status` 提供本地同步状态，`health://device-profile` 列出当前后端能力。Gadgetbridge 模式不注册小米健康专用的实时心率、佩戴状态或日程控制工具。

日报和明细直接使用缓存；最近状态默认 `prefer_fresh`，心率较旧时会尝试导出并拉取，手机离线时保留缓存和明确失败原因。显式 `cached` 不联系手机，`require_fresh` 无法满足年龄要求时返回 `freshness_unmet`。需要先让手环同步记录时，调用 `sync_health(refresh_app=true)`。导出成功、拉取成功与产生新观测分别记录。

## 同步与恢复

安装阶段须在 Gadgetbridge 开启活动同步、数据库导出的 Intent 触发及完成广播，并配置可读取的导出位置。运行同步时 Gadgetbridge 应在运行，主动手环同步还需要手环连接。

```bash
MIBAND_HEALTH_CONFIG=/path/to/private/gadgetbridge.json .venv/bin/miband-health now
MIBAND_HEALTH_CONFIG=/path/to/private/gadgetbridge.json .venv/bin/miband-health sync --sync-device --timeout 90
MIBAND_HEALTH_CONFIG=/path/to/private/gadgetbridge.json .venv/bin/miband-health daily 2026-09-21
```

传输限定到配置的 ADB 设备；未配置序列号时只在恰有一台已授权设备时自动选择。不会自动提升权限、开启无线调试、打开 App 或操作配对弹窗。

同步监听本轮 Gadgetbridge 完成事件，校验手机/电脑文件哈希及完整查询 schema 后，原子替换缓存。传输、超时或校验失败保留此前文件；并发请求共用写锁。状态文件与缓存均在本地数据目录，临时文件和监听进程在结束时清理；不保存完整手机日志。

手机中的导出文件由 Gadgetbridge 更新，电脑库由同步后端更新。已有小米健康 timer 不会因为某个 MCP 客户端选择本配置而自动切换后端。一般用户只在电脑开着、调用 MCP 时同步，不需要为本路径新装 Tailscale 或常驻服务。

## 实机验收

2026-09-21，Linux、Gadgetbridge 0.94.0、数据库 schema 140、小米手环 10，经真实 stdio MCP 客户端完成：

- 发现上述四个工具；请求活动同步并确认本轮完成，再请求导出并确认成功。
- 同步前后所统计的数据记录从 154 增至 155，最近观测从 03:52 推进到 03:53 CST；心率新鲜度满足要求。
- 手机与电脑文件均为 884,736 bytes，完整 SHA-256 一致；缓存身份与查询打开的快照一致。
- 最近状态、日报、时间序列查询通过。
- 使用不存在的手机序列号模拟连接不可用，同步报告失败，旧缓存哈希不变且查询成功。
- 同步结束后无遗留日志监听、拉取临时文件或本轮 MCP 服务进程。后续客户端仍按需启动服务。
- MateBook 的 Codex、Claude 配置已切换到私有 Gadgetbridge profile，原配置留有备份；Claude 连接检查通过，新开的 Codex 会话实际调用 `get_current_state(freshness="cached")` 成功，返回来源 `gadgetbridge`、观测时间 03:53 CST、状态 `fresh`。
- 全仓 133 项测试通过，覆盖原后端回归、查询映射、同步失败保护及 MCP 路由；wheel 构建通过。

本阶段没有使用 root/SSH/Frida。手机本身是已有开发配置的测试机，因此首次无预先授权安装、Windows、无线恢复和长期熄屏稳定性仍需另行验收。
