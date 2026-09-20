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

在受支持的 Android 14、国内版小米运动健康 3.59.1 与小米手环 10 环境中，已通过真实 stdio MCP 客户端验证：

- 原生后台同步在 App 后台、屏幕关闭时完成，以 App 的 `lastSyncDataTime` 推进、设备恢复 connected/idle 为确认依据；数据库导出与增量入库随后成功。
- 电量、充电状态和三类只读监测设置均从手环实时返回；人为断开当前设备连接后，下一次查询通过当前设备 ID 自动重连成功。
- 单次心率测量确认 START、收到新读数并确认 STOP，结束后无运行时残留。
- 闹钟和提醒分别完成创建、独立读取、修改、独立读取、删除及最终空列表核对；实际协议 ID 允许从 0 开始，写操作仍只发送一次并以读回为准。
- 设备请求使用原生 `OnSyncCallback` 返回的 packet，不修改 App 的既有 Java 方法；迟到 callback 以逐请求 token 隔离。连续独立运行时读取与清理通过。

佩戴、睡眠及活动状态的当前 getter 在该版本上仍未取得有效响应，返回 unknown；这不表示硬件不支持，也不影响 `get_band_status` 对连接、电量和监测设置独立判定新鲜。全仓 141 项测试与 wheel 构建通过，wheel 已核对包含三个运行时 JS 资源且不含开发机私有标识。Gadgetbridge 四工具的数据链另有完整 USB 实测记录。
