# MiBand MCP

让电脑上的 agent 读取小米手环的健康记录。当前支持小米手环 10，提供最近状态、日报、历史明细与数据同步；每项结果区分观测时间、拉取时间和缺失数据。

当前为首个公开测试版本（alpha）。主要验通路线是国内版安卓手机、Gadgetbridge、USB 和 Linux 电脑；其他平台及无线方式以各文档的验证范围为准。

使用效果如下： 
<img width="1267" height="551" alt="image" src="https://github.com/user-attachments/assets/042ce8ba-3d43-4bea-afce-a5cd912b1744" />

<img width="1468" height="480" alt="image" src="https://github.com/user-attachments/assets/0dd89461-bb66-4670-8b37-55ad51adae71" />

<img width="1258" height="866" alt="image" src="https://github.com/user-attachments/assets/de649619-9000-464a-9527-1461385ba049" />


## 让 agent 帮你配置

把本仓库交给能够操作电脑的 agent，并请它读取 [配置 skill](skills/miband-setup/SKILL.md)。它会检查设备、解释需要的选择，完成电脑配置，并逐步引导手机操作。也可以选择让 agent 帮忙操作手机；配对确认仍由你在手机和手环上完成。

可以直接把下面这段话交给 agent：

> 请按 https://github.com/kirisawa-subaru/miband-mcp 的配置 skill 帮我安装手环 MCP。先检查我的电脑和手机是否适用，默认用 USB 连接。电脑上的安装和配置由你完成，需要我在手机或手环上操作时，一步步告诉我。最后通过 MCP 实际同步并查询数据。

当前无需 root 的路线需要：

- 安卓手机和小米手环 10；苹果手机不适用。
- Linux 或 macOS 电脑；Windows 尚未适配。
- 支持数据传输的 USB 线，可先找手机包装盒里的原装线。
- 接受改用 Gadgetbridge 管理手环。官方 App 的旧历史不会随配对密钥自动迁移过去。

先用 USB 完成连接。电脑开着、手机接上线时按需同步，无需先安装 Tailscale。无线及跨网络连接仍需单独配置和验收。

## 实现与开发

同一个 `miband-health-mcp` 入口按配置选择后端，数据分别缓存：

| 后端 | 数据通道 | 接口 |
|---|---|---|
| Gadgetbridge | 已授权 ADB、App 导出、电脑 SQLite 缓存 | 最近状态、日报、明细、同步 |
| 小米运动健康（实验） | 已有 root/SSH 环境、官方 App 数据库与内部接口 | 查询、后台同步及设备控制已在指定版本实机验收；佩戴、睡眠和活动状态仍返回 unknown，App 升级后需重新验收 |

- [Gadgetbridge 安装与配置](doc/gadgetbridge-mcp.md)
- [手机安装实测与已知体验问题](doc/gadgetbridge-e2e.md)
- [现有小米运动健康部署](doc/xiaomi-health-mcp.md)

使用 `uv sync --locked` 安装依赖；需要小米健康进程内控制时加 `--extra xiaomi`。单元测试运行 `.venv/bin/python -m unittest discover -s tests`。原 `mibandctl` CLI 保留，新的 MCP 入口是 `miband-health-mcp`。

密钥、健康数据库、手机日志和用户配置仅保存在用户本机私有目录。尚未覆盖没有预先调试授权的首次安装测试；安装 agent 按实际机型引导连接，遇到版本差异可提交问题反馈。

## 许可证与来源

本项目采用 [AGPL-3.0-or-later](LICENSE)。Gadgetbridge 提供了设备支持、数据格式与协议研究基础，参考来源和第三方组件说明见 [NOTICE](NOTICE)。
