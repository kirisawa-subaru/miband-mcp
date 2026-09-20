# 接入路径与实现状态

## 当前仓库状态（2026-09-21）

本 skill 随仓库分发。电脑端已有双后端查询与 USB 同步实现；面向普通用户的首次安装体验仍需独立验收。

- `miband-health-mcp` 通过显式配置选择 `gadgetbridge` 或 `xiaomi_health`。Gadgetbridge 提供最近状态、日报、明细查询和同步四个工具；不注册官方 App 专用的主动测量与日程工具。安装命令、配置字段和工具契约见仓库 [Gadgetbridge MCP 后端](../../../doc/gadgetbridge-mcp.md)。
- Gadgetbridge 后端通过已授权 ADB 请求同步、导出并拉取到电脑；校验后原子替换本地缓存，失败保留旧库。电脑端同步不依赖 root、SSH 或 Frida。当前支持 Linux/macOS，Windows 尚未适配；先识别用户电脑，不能默认所有电脑都可安装。
- 2026-09-21 已在现有开发设备上实测 Gadgetbridge 0.94.0 配对、活动同步、数据库导出及 USB 拉取；重复拉取带回新记录。普通用户首次调试授权、局域网与跨网恢复尚未验收，不能由这次结果推断。
- 原 `mibandctl` 仍可通过 `MIBAND_DB_PATH` 读取电脑上的 Gadgetbridge 库；它与新的 MCP 配置入口分开，不要只验这个 CLI 就宣布 MCP 完成。
- 小米健康后端要求显式配置用户自己的 SSH 主机和设备序列号，不带开发者设备默认值。Android 14、国内版小米运动健康 3.59.1 与小米手环 10 已实测后台同步、电量与监测设置读取、断线重连、单次心率和日程读写；佩戴、睡眠及活动状态 getter 在该版本仍返回 unknown。内部接口受 App 版本影响，升级后需重新验收。
- 无线自动发现/重连、无需 root 的跨网络传输仍待实现与验收。安装 agent 不能为了宣布成功而虚构能力。

## 国内版、无需 root：Gadgetbridge

适用起点：安卓手机已经通过国内版小米运动健康 `com.mi.health` 绑定小米手环 10，且用户接受改用 Gadgetbridge 管理手环。

首次切换前确认官方 App 中数据已同步，说明通知等权限需要重新配置；现有官方 App 历史不会因为填写密钥就自动进入 Gadgetbridge。保留已有账号和历史，不把重置、解绑、卸载或降级作为常规准备动作。

### 取得配对密钥

先检查已有日志是否可读且包含目标设备的记录。能够直接读取时先解析，无需以出现日志迁移弹窗为前置条件。需要迁移/导出时，公开的国内版路径是：在小米运动健康同步设备，进入“我的 → 关于”，连续点击橙色 Logo，确认日志迁移。具体按钮与次数根据当前版本和页面识别；校准控件后仍无预期响应，应转查现有文件或当前版本的导出入口，不重复多轮盲点。

agent 操作模式下自行完成界面步骤；手动模式下将上述过程拆成逐页、逐动作的提示。普通 ADB 文件访问如果读不到日志，使用 App 的迁移/导出入口，或引导用户将导出包保存到可传给电脑的位置。不要把“无 root 无法读 App 私有目录”误判为不能通过 App 导出。

可先检查的日志目录是 `/sdcard/Android/data/com.mi.health/files/log/`；迁移文件的实际位置以页面或导出结果为准。文件名可能包含 `XiaomiFit.device.log`、`XiaomiFit.main.log`、`Transfer.device.log`，也可能是压缩包中的文本。

在用户电脑本地解析 `encryptKey`、`token`、`authKey`、`huamiAuthKey` 等字段，结合设备标识与日志时间筛选。检查上述实际存在的主日志、设备日志及相关轮转文件，不能因设备日志的某个字段为空就断定无密钥。32 位十六进制格式只证明它是候选值，须通过目标手环实际认证验证。找不到时核对 App 版本、是否同步以及日志迁移结果；不要反复使用同一个失败密钥，也不要直接要求用户 root、降级或重置。

拉取的日志与解压文件放在本次安装的私有临时目录，处理完成后清理本次副本，保留用户原文件。长期只保存连接需要的配置，并向用户说明本地保存位置；不要将完整日志随开源项目或安装记录分发。

表盘自定义工具教程提供了操作依据，但安装此工具不是本 skill 的必经步骤；能够读取导出日志时，由电脑端解析承担这部分工作。

### 切换与导出

按 Gadgetbridge 对该型号的指引进入连接流程，需在手环上选择“连接新手机”时由用户完成。避免两 App 争抢蓝牙连接，可在切换时停止官方 App；不要在官方 App 中解绑或恢复手环出厂，这会使已取到的密钥失效。

引导“连接新手机”时提前说明：仍复用已提取的密钥；如果出现二维码，保持在该界面即可，不用扫码，接下来由 Gadgetbridge 搜索连接。出现其他提示时先核对文字，不把清除数据或恢复出厂当作进入等待连接的同义操作。正式配对时遵循主 skill 的人工双端确认步骤。

在 Gadgetbridge 中先打开“启用自动导出”，再选择“导出位置”；开关关闭时相关设置可能不可用。系统保存界面中的位置由用户选择，完成后 agent 通过获授权的截图/UI 信息读取位置并验证实际文件，不要求用户抄写路径。用应用的立即导出或已授权的导出 Intent 实测后拉到电脑；先建立基线，再验证再次同步与导出是否推进记录时间，不能仅以文件存在判断成功。

公开 Intent 可以触发同步和数据库导出，需在 Gadgetbridge 中启用对应权限。按当前版本核对包名、动作名与完成事件，不把广播发送成功当成设备同步成功。手动操作偏好若禁止此类自动手机动作，也应改为引导用户操作同步/导出按钮。

已有记录查询和主动测量分别核对：Gadgetbridge 的公开自动化接口目前不能直接提供本项目小米健康后端的一次性实时心率测量；闹钟接口也不等同于设备端列表读回。只暴露后端实际提供并验证过的能力。

### 接入电脑与 MCP 客户端

按仓库 [配置文档](../../../doc/gadgetbridge-mcp.md) 安装电脑依赖，创建用户私有的 JSON 配置。agent 从实际连接中读取手机序列号、导出位置及数据库中的手环标识，写入 `backend: gadgetbridge` 和相应字段；没有配置文件时默认仍是旧的小米健康后端，不能遗漏后端选择。

保留客户端其他服务，仅为本服务设置 `MIBAND_HEALTH_CONFIG` 和实际可执行文件路径。存在同名服务时先备份，再更新该项。客户端如需重连或新会话才会加载配置，用日常语言告知用户。

先通过 MCP 发现四个工具，再调用 `sync_health(refresh_app=true)`，确认手环同步、数据库导出、校验与拉取成功。随后实际调用最近状态、日报与明细；检查返回来源为 `gadgetbridge`，分别记录最新观测时间与拉取时间。首次使用没有睡眠或运动记录是允许的，返回缺失即可。

在可恢复的条件下验证手机不可用时仍能查询已有缓存。结束时告诉用户：手机与手环保持连接，电脑使用时插上数据线并保持 Gadgetbridge 运行；若数据偏旧，agent 先检查同步状态，用户无需手动管理数据库。

## 已有 root：小米运动健康

仅在用户已有适用环境、愿意使用该路径且发行包实际支持时选用。检查用户的手机、App 包名/版本、传输入口和数据位置，配置用户自己的参数。

普通记录导出与需要进程内调用的主动控制分别安装和验收，具体配置见 [小米运动健康后端](../../../doc/xiaomi-health-mcp.md)。不要为了满足安装流程引导普通用户 root；只在核对用户配置、设备身份及 arm64 环境后安装可选运行时。

## 上游依据

以下是流程的资料入口，不能替代目标手机实测。菜单或数据格式变化时查相关入口，无需让普通用户自行阅读技术文档。

- [米坛：国内版日志迁移与 AuthKey 读取](https://wiki.bandbbs.cn/Guides/watchface_custom_tool/watchface_custom_tool-install.html)
- [Gadgetbridge：配对、无 root 日志读取及密钥使用](https://gadgetbridge.org/basics/pairing/huami-xiaomi-server/)
- [Gadgetbridge：手环 10 设备页](https://gadgetbridge.org/gadgets/wearables/xiaomi/#mi-band-10)
- [Gadgetbridge：数据库自动导出](https://gadgetbridge.org/internals/automations/auto-export/)
- [Gadgetbridge：同步与导出 Intent](https://gadgetbridge.org/internals/automations/intents/)
- [Android：ADB 与首次授权](https://developer.android.com/tools/adb)
- [Android：设备与无线调试连接](https://developer.android.com/studio/run/device)
- [Tailscale：SSH 支持的平台与边界](https://tailscale.com/docs/features/tailscale-ssh)
