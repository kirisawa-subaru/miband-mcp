# 小米运动健康后端

这是面向已有 root/SSH 环境的可选后端。普通用户优先使用 [Gadgetbridge 路线](gadgetbridge-mcp.md)，无需安装本页的运行时。两种后端使用独立缓存；同一手环切换管理 App 时，避免两个 App 争抢连接。

## 配置

本后端针对国内版 `com.mi.health`，通过 SSH 和 root 只读导出手机数据库。手机需已有 SSH 服务、`su` 与 Termux Python；电脑需可通过已登记主机密钥的 SSH 公钥登录。主动控制另外依赖 arm64 Android 和受支持的官方 App 内部接口。

在本机私有目录创建配置，例如 `~/.config/miband-health/xiaomi.json`：

```json
{
  "backend": "xiaomi_health",
  "ssh_host": "YOUR_PHONE_SSH_ALIAS",
  "device_serial": "YOUR_PHONE_SERIAL",
  "timezone": "Asia/Shanghai"
}
```

不提供开发者手机默认值。安装 agent 应从用户已授权设备读取身份、写入配置，并给文件设置仅当前用户可读写的权限。所有手机访问均先核对序列号。

```bash
uv sync --locked --extra xiaomi
export MIBAND_HEALTH_CONFIG="$HOME/.config/miband-health/xiaomi.json"
.venv/bin/miband-health sync --timeout 120
.venv/bin/miband-health now --freshness cached
```

客户端配置与 [Gadgetbridge 文档](gadgetbridge-mcp.md) 使用相同入口，只需选择本配置文件。默认缓存为 `~/.local/share/miband-health/health.sqlite`；首次导入近 30 天，后续回看 72 小时处理迟到、修订和删除。

## 工具

- 公共查询：`get_current_state`、`get_daily_report`、`query_health`、`sync_health`。
- `get_health_status`：健康记录，加上手环报告的佩戴、睡眠及活动状态；未知与缺失独立表示。
- `get_band_status`：连接、电量、充电和只读监测设置。
- `measure_heart_rate`：开始一次测量，收到读数或超时后停止。时间表示电脑收到结果的时间。
- `get_band_schedule`、`set_band_alarm`、`set_band_reminder`、`delete_band_schedule`：读取及修改日程，修改后读回核实。未知写入结果需核对，不能盲目重复。

普通同步仅拉取已有记录；`sync_health(refresh_app=true)` 使用运行中的官方 App 内部接口请求手环同步。需要该接口和设备控制时，再安装可选运行时：

```bash
.venv/bin/python deploy/install-measurement-runtime.py
```

安装器下载固定版本的 Frida arm64 服务端并校验哈希，不安装开机服务；控制调用按次启动并清理进程与隧道。内部接口依赖官方 App 版本，升级后需要重新验收。

## 可选定时拉取

仅在用户需要后台周期拉取时启用 `deploy/miband-health-sync.service` 与 `.timer`。先按实际仓库位置调整模板的 `WorkingDirectory` 和 `ExecStart`；模板通过 `MIBAND_HEALTH_CONFIG` 显式选择上述私有配置。Gadgetbridge 默认按需同步，不需要安装此服务。

定时任务每约五分钟拉取一次，查询仍读本地缓存；失败保留旧数据。安装后通过 `systemctl --user status miband-health-sync.timer` 检查状态，用 `systemctl --user disable --now miband-health-sync.timer` 停用。

## 验证范围

历史记录导出、重复入库、实际 MCP 查询与主动心率测量已有开发机实测。内部后台同步替换、佩戴/活动状态、日程控制已通过本地回归，仍待完整实机验收；本版本把它们保留为实验能力。Gadgetbridge 四工具的数据链另有完整 USB 实测记录。
