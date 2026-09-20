# MiBand-connection — v0 Planning

## 目标

向 LLM 暴露一个 Mi Band 10 健康数据接口，让后续对话中验证关于 用户 物理状态的 claim 时可以查到结构化数据。**不是**生成健康报告，是数据源。

接口边界：

- **CLI = stable surface**。所有数据访问点常驻，不随用例变动剪枝。
- **Skill = curation layer**。SKILL.md 策展哪些 CLI 命令暴露给 LLM。两层独立演化。

## 数据流与延迟模型

CLI 端只关心 DB 文件落到本机后的事，同步链路（手环→手机→云→Mac）不是 CLI 的职责。

CLI 需要假设的事实：

- DB 路径：`~/.local/share/miband-gadgetbridge/Gadgetbridge.db`
- 数据延迟：分钟到小时级。LLM 默认按"小时级数据"理解。
- 文件更新模型：覆盖式同步（不是 append-log）。同 mtime 等价于"自上次以来无新数据"。
- 新鲜度信号：`mtime(Gadgetbridge.db)` + `max(TIMESTAMP)`。CLI 只报这两个数字，不解读。
- 需要更新鲜数据时，让 LLM 提示用户在手机端手动触发同步后重试。

## CLI Surface（v0 = 6 commands）

```
mibandctl health             链路状态（无业务数据）
mibandctl now                当下小快照：最新 HR + 最近 1h 步数 + 电量 + freshness
mibandctl sleep [N=1]        最近 N 夜，sleep_time + stage 分布
mibandctl hr [N=1]           最近 N 天心率，默认小时桶（resting/min/max/avg）
mibandctl activity [N=1]     最近 N 天步数/距离/卡路里，默认小时桶
mibandctl daily [N=1]        最近 N 天 XIAOMI_DAILY_SUMMARY 整行
```

通用参数：

- `--start ISO --end ISO`：精确区间，覆盖 `[N]` 简写
- `--minute`：仅 `hr` / `activity` 有效，下钻到分钟级裸数据

域切分原则：每个命令一个数据轴，互不污染。**不做**多域揉合的"观点视图"。

## 输出格式

JSON 到 stdout。

```json
{
  "schema": {"hr": "bpm", "steps": "count", "distance": "m", "sleep": "min"},
  "freshness": {
    "db_mtime": "2026-05-05T18:58:00+08:00",
    "data_latest": "2026-05-05T18:00:00+08:00",
    "data_age_min": 62
  },
  "tz": "+08:00",
  "data": ...
}
```

- units 在 `schema` 块声明一次，`data` 块裸数字
- 不做 `status: healthy/stale/broken` 预解读，给 `data_age_min` 让调用方判断
- 不做 Markdown 表

## 数据源 schema

DB: `~/.local/share/miband-gadgetbridge/Gadgetbridge.db`，SQLite user version 128。
Device: `Xiaomi Smart Band 10` (`M2457B1`), firmware 3.1.11。

v0 涉及的表：

| 表 | 用途 | TS 单位 |
|---|---|---|
| `XIAOMI_ACTIVITY_SAMPLE` | 分钟级 HR / steps / distance / calories | 秒 |
| `XIAOMI_DAILY_SUMMARY_SAMPLE` | 日级汇总 + TZ 字段 | 毫秒 |
| `XIAOMI_MANUAL_SAMPLE` | 手动测量（TYPE=17 是 HR） | 毫秒 |
| `XIAOMI_SLEEP_TIME_SAMPLE` | 整晚 sleep totals | 毫秒 |
| `XIAOMI_SLEEP_STAGE_SAMPLE` | sleep stage transitions | 毫秒 |
| `BATTERY_LEVEL` | 电量 | 秒 |

## 关键归一化（必做，在 extractor 边界）

vendor 怪癖在最底层处理掉，不向上传染：

1. **时间戳归一**：activity / battery 是秒，其他是毫秒 → 统一转 ISO-8601 `+08:00`
2. **缺测哨兵 → null**：
   - `HEART_RATE`: `0 → null`
   - `STRESS`: `0, 255 → null`
   - `SPO2`: `0, 255 → null`
   - `ENERGY`: `-1 → null`
3. **Sleep stage int → string**（Band 10 / `SleepDetailsParser.decodeStage` 源码核实）：
   - `0 → "not_sleep"`
   - `1 → "na"`（fallback / 未知）
   - `2 → "deep"`
   - `3 → "light"`
   - `4 → "rem"`
   - `5 → "awake"`
4. **TZ 解码**：`XIAOMI_DAILY_SUMMARY_SAMPLE.TIMEZONE` 是 15min 块，`32 → +08:00`
5. **`STANDING` 是 24-bit 小时位掩码**（`DailySummaryParser` 源码核实），不是计数也不是时长。bit 0 = 00:00-01:00。CLI 解码成 `standing_hours: [int]` 数组。例：`28 = 0b11100 → [2,3,4]`；`7880732 = 0x787F1C → [2,3,4, 8-14, 19-22]`。
6. **`IS_AWAKE` 不是"是否清醒"，是 `!isSleepFinish` 标记**（源码核实）。归一化为 `session_state` 三态：
   - `0 → "final"`（写入时 session 已最终化）
   - `1 → "in_progress"`（写入时仍在进行）
   - `NULL → "unspecified"`（type-16 summary 分支不写此字段，通常对应 nap / 短睡眠窗口）
7. **`XIAOMI_SLEEP_TIME_SAMPLE` 中 `TOTAL>0` 但 `DEEP=LIGHT=REM=AWAKE=0` 的 row 是合法 nap / 未分类窗口**（源码核实，type-16 summary 分支产物）。CLI 不过滤、不合并、原样透传。LLM 自己判 nap 还是断觉。

## Skill 设计

SKILL.md 短描述，触发场景偏 reflective（LLM 内省时主动查，不只是用户问到才查）：

```
Personal health-sensor feed (HR / steps / sleep / stress) from the user's
Mi Band 10. Data freshness ~1h. Use to verify claims about the user's
physical state, or when he asks directly.
```

不写 if-then 触发示例。不在 skill body 里 pre-bake JSON（永远 stale by design）。

## 不做清单（v0 显式 out-of-scope）

- **多设备 / 多厂商 abstraction**。单用户单手环，硬编 XIAOMI_*。
- **stress / spo2 提取**。表字段全 0/255，手环未启用对应采样，等真有数据再加 schema。
- **workout 提取**。`BASE_ACTIVITY_SUMMARY` 空，`XIAOMI_ACTIVITY_SAMPLE.RAW_KIND/INTENSITY` 全 0。
- **观点视图**（recovery-status / fatigue-score / anomaly-today）。LLM 自行 compose。
- **预定义 status 字符串**（healthy / stale / broken）。给原始 age 即可。
- **`--fields` 过滤参数**。每个域命令默认返回全字段。

## v1+ 候选

- workout 提取（等 `RAW_KIND` 出现非 0）
- stress / spo2（等手环对应采样开启）
- 多日趋势复合接口（如确实证实 LLM 反复 compose 同一组合，再下沉）
- **read 命令读 archive**：当前 hr/sleep/activity/daily/now 都还在读 source。要么改成"archive 存在则读 archive、不在则 fallback source"，要么 ATTACH 双库 union（更准但更复杂）。
- **archive sync 自动化**：launchd 每小时跑一次 `mibandctl archive sync`，跟手机端 xx:55 导出错开 5-10 分钟。

## 已完成（v1）

- **Mac 端 append-only 历史归档**（2026-05-07）：`mibandctl archive sync` / `mibandctl archive info`。存于 `~/Library/Application Support/mibandctl/archive.db`，env `MIBAND_ARCHIVE_PATH` 可覆盖。利用源表本身 `PRIMARY KEY ON CONFLICT REPLACE` 行为去重，sync 幂等。Schema drift 在列层面检测并报告（不自动迁移）。`user_version` 镜像，方便后续检测固件升级带来的 schema 变化。
