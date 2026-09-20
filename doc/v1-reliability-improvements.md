# MiBand-connection — v1 Reliability Improvements

Date: 2026-06-01

## 背景

这次改动集中在 `mibandctl` 的可靠性边界：时间窗口语义、SQLite 只读连接、多个同步源的新鲜度选择，以及 sleep 查询的日期归属。

CLI 的定位不变：它仍然只负责把 Gadgetbridge SQLite DB 中的数据稳定地转成 JSON，不做健康状态判断，不生成报告。改动目标是让同一条命令在边界输入、同步源切换、路径包含特殊字符、睡眠跨日等情况下行为更可预测。

## 主要改进

### 1. 统一时间窗口解析

`hr`、`activity`、`daily`、`sleep` 现在共用 `db.resolve_window_seconds()` / `db.resolve_window_millis()`。

行为变化：

- `[N]` 必须 `>= 1`，避免 `N=0` 产生未来起点。
- `--start` 和 `--end` 必须成对出现；不再静默忽略单边显式窗口。
- 显式窗口要求 `start < end`。
- ISO `Z` 后缀会按 UTC 解析。
- CLI 会把这些 usage error 输出成 JSON error，并返回 exit code 2。

示例：

```bash
mibandctl hr 0
# {"error": "n must be >= 1"}

mibandctl hr --start 2026-06-01T00:00:00+08:00
# {"error": "--start and --end must be provided together"}
```

### 2. sleep 改为按 wakeup date 归属

旧逻辑为了包含前一晚入睡的记录，把 sleep 查询窗口扩大到 `N + 1` 天。这会让 `sleep 2` 在某些情况下包含三天 wakeup 的 sleep rows。

新逻辑改成：

```sql
WHERE WAKEUP_TIME >= ? AND WAKEUP_TIME <= ?
```

也就是说，sleep session 归属于醒来的日期。前一晚开始、今天醒来的 sleep 仍会被包含；昨天醒来的 sleep 不会因为 bedtime 落在扩展窗口内而混进来。

Sleep stage rows 仍然按已选中的 sleep session 范围读取：

```text
[sleep TIMESTAMP, WAKEUP_TIME]
```

因此，前一晚的 stage transition 不会被 nominal day start 错误截断。

### 3. SQLite read-only URI 集中处理

新增 `db.sqlite_ro_uri(path)`，统一构造只读 SQLite URI：

```py
f"{path.expanduser().resolve().as_uri()}?mode=ro"
```

它替代了手写的 `file:{path}?mode=ro` 形式，并被用于：

- `db.connect()`
- `archive.sync()`
- `archive.info()`
- freshness candidate scoring

这解决了路径中包含空格、`#`、`?` 等 URI 特殊字符时的连接风险，也避免只读连接构造散落在各处。

### 4. 多个同步源按数据新鲜度选择

`DEFAULT_DB_PATH` 扩展为 `DEFAULT_DB_PATHS`。当没有设置 `MIBAND_DB_PATH` 时，CLI 会在多个候选 Gadgetbridge DB 中选择数据时间戳最新的一个。

规则：

- `MIBAND_DB_PATH` 仍然拥有最高优先级，设置后进入单源模式。
- 默认候选只考虑存在的文件。
- 选择 `XIAOMI_*` / `BATTERY_LEVEL` 中最新 sample timestamp 最大的 DB。
- 如果 timestamp 读不到，则回退到第一个存在的候选。
- `db_path()` 每个进程只选择一次，保证同一次 CLI 调用中 freshness 和业务查询使用同一源。

`freshness` 输出现在包含：

- `db_source`
- 多候选情况下的 `candidates`
- 每个候选是否存在、是否被选择、数据年龄估计

### 5. freshness 覆盖更多 sleep 新鲜度信号

`latest_data_ts_seconds()` 和 DB candidate scorer 都把 `XIAOMI_SLEEP_TIME_SAMPLE.WAKEUP_TIME` 纳入 freshness 计算。

这比只看 sleep bedtime 更符合用户对“最近一次 sleep 数据”的直觉：一条昨晚开始、今天早上醒来的 sleep session，应当把今天早上的 wakeup 作为新鲜度信号之一。

## 测试覆盖

新增 `tests/test_db.py`，覆盖：

- SQLite read-only URI 可处理空格、`#`、`?`，且确实只读。
- 默认候选 DB 会选择 freshest source。
- `MIBAND_DB_PATH` override wins。
- env path 中的 `~` 会展开。
- 单边显式窗口被拒绝。
- 反向或空显式窗口被拒绝。
- ISO `Z` 后缀可解析。
- `N=0` 被拒绝。
- CLI usage error 输出 JSON。
- `sleep.run()` 的实际 JSON 输出会包含醒在目标窗口内的前一晚 sleep session，并包含该 session 前一晚的 stage row。

## 最终验证

本次合并前使用的 gate：

```bash
grep -R --exclude-dir='__pycache__' 'file:{' -n mibandctl
grep -R --exclude-dir='__pycache__' 'mode=ro' -n mibandctl
python3 -m unittest discover -s tests -v
python3 -m compileall -q mibandctl tests
git diff --check
```

验证结果：

- `file:{` 无命中。
- `mode=ro` 只出现在 `sqlite_ro_uri()` helper。
- `unittest` 11 tests passed。
- `compileall` passed。
- `git diff --check` passed。

## 后续非阻断项

- 把 freshness 的私有 helper 跨模块引用整理成公开的小函数，例如 `candidate_db_freshness()`。
- 引入 CLI-specific usage exception，避免长期 catch broad `ValueError`。
- 增加 malformed/missing-table freshness tests。
- 如果默认日期窗口继续扩展，考虑给 `days_ago_seconds()` 注入 clock，以便做确定性测试。
