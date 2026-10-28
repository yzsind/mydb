# MyDB 短视频脚本：把时钟拨到 2040 年，MySQL 装不上、MyDB 一切正常

- 版本：v1.0（2026-10-28）
- 成片规格：75s ±2s ｜ 16:9（附 9:16 竖版裁切建议）｜ 中文旁白 + 中文字幕（关键词高亮）
- 定位：热点事件型实测短视频（抖音/视频号/B站动态），区别于 120s 产品正片，主打"实测冲击力"
- 数据可信度：本文档中**每一段终端画面均已于 2026-10-28 在本机实测跑通**，可直接照抄录屏：
  - MySQL：`26.7.0`（Homebrew，2026-10 本机最新版），路径 `/opt/homebrew/bin/mysqld`
  - MyDB：`8.0.46-mydb-1.0.0-20261028`（release 构建）
  - 测试方法：libfaketime `faketime -f "+14y"`（时钟拨至 2040-10-24，时间正常流动）
  - 测试数据目录：`/tmp/mydb/y2040-mysql`、`/tmp/mydb/y2040-mydb`（保留可复用）
- 官方佐证：MySQL Bug [#119318](https://bugs.mysql.com/bug.php?id=119318)（2025-11：系统时间超过 2038 后安装/初始化失败）

---

## 1. 标题与封面候选

| 方向 | 候选 |
|------|------|
| 悬念型（推荐） | 《把电脑时间调到 2040 年，MySQL 当场装不上了》 |
| 事件型 | 《我们提前 14 年重启了 MySQL：2038 不是科幻》 |
| 硬核型 | 《2038 问题实测：MySQL 数据字典当场殉爆，MyDB 全绿》 |

封面画面：左边红色终端报错（`Data Dictionary initialization failed`），右边绿色终端（`mydb listening on ...`），中间大字 "2040"。

---

## 2. 总时间线

| # | 章节 | 时间码 | 时长 | 核心一句话 | 情绪 |
|---|------|--------|------|------------|------|
| S0 | Hook | 00:00–00:05 | 5s | 提前 14 年，时钟拨到 2040 | 悬念起手 |
| S1 | 实验设置 | 00:05–00:15 | 10s | libfaketime 把时钟拨到 2040，两边同台 | 冷静铺垫 |
| S2 | MySQL 三连败 | 00:15–00:30 | 15s | 装不上 → 首启失败 → DDL 全挂 | 红色冲击 |
| S3 | MyDB 全绿 | 00:30–00:50 | 20s | 启动/建表/2060 直插/CREATE USER/重启恢复 | 绿色爽感 |
| S4 | 原理 15 秒 | 00:50–01:05 | 15s | 32 位秒 vs 64 位微秒 | 信息增量 |
| S5 | CTA | 01:05–01:15 | 10s | 2038 距今只剩 12 年 | 收束召唤 |

---

## 3. 分段脚本

### S0 · Hook（00:00–00:05，5s）

**画面**：黑场 → 红色报错终端快速闪现（S2 的 `Data Dictionary initialization failed` 截帧）→ 大字 "2040" 砸屏。

**旁白**（约 20 字）：
> "把电脑时间拨到 2040 年，MySQL 直接装不上了。"

**字幕关键词**：`2040` `MySQL 装不上`

### S1 · 实验设置（00:05–00:15，10s）

**画面**：终端录屏，逐行执行：

```bash
brew install libfaketime
date                                    # 2026-10-28（真实时间）
faketime -f "+14y" bash -c date         # 2040-10-24（假时间，继续流动）
```

**旁白**（约 40 字）：
> "这不是特效。用 libfaketime 把系统时钟整整拨快 14 年，而且时间照常流动——今天起，我们活在 2040 年。"

**字幕关键词**：`libfaketime` `+14y` `2040-10-24`

### S2 · MySQL 三连败（00:15–00:30，15s）

**画面**：三段报错快切（每段约 5s，红框高亮关键行）：

1. 全新初始化：
```bash
faketime -f "+14y" mysqld --no-defaults --initialize-insecure \
  --datadir=/tmp/mydb/y2040-mysql
# [ERROR] [MY-010020] Data Dictionary initialization failed.
# [ERROR] [MY-013236] The designated data directory ... is unusable.
```
2. 用 2026 年初始化好的数据目录启动：
```bash
# [ERROR] [MY-010021] Failed to update plugin metadata in dictionary tables.
# [ERROR] [MY-010119] [Server] Aborting
```
3. 数据字典完全就绪后再启动（进程能起），然后：
```bash
mysql> CREATE DATABASE d40;
# ERROR 3507 (HY000): Failed to update schemata dictionary object.
# 错误日志根因：MY-001292 Incorrect datetime value:
#   '20401024083951' for column 'created'
```

**旁白**（约 60 字）：
> "MySQL 最新版，三连败：全新安装，当场失败；老数据目录首次启动，直接中止；就算库起了，建个新库也报错——元数据里的时间列，越界了。官方 bug 单 119318，实锤。"

**字幕关键词**：`Data Dictionary failed` `Aborting` `ERROR 3507` `Bug #119318`

### S3 · MyDB 全绿（00:30–00:50，20s）

**画面**：同一假时钟下，绿色系终端录屏（关键行打光/放大）：

```bash
faketime -f "+14y" mydb --host 127.0.0.1 --port 4940
# mydb listening on 127.0.0.1:4940; ssl=enabled (ephemeral
# self-signed certificate); log_bin=OFF

mysql> SELECT NOW(6);
# 2040-10-24 16:41:06.917621        ← 时钟正确

mysql> CREATE TABLE d40.t40(ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, a INT);
mysql> INSERT INTO d40.t40(a) VALUES(1);        -- 默认值写入 2040，无越界
mysql> INSERT INTO d40.t40(ts,a) VALUES('2060-08-08 08:08:08', 2);
mysql> SELECT ts FROM d40.t40;
# 2040-10-24 16:41:07
# 2060-08-08 08:08:08              ← 2060 直插成功，没有 2038 上限

mysql> CREATE USER u40 IDENTIFIED BY 'x12345678';   -- OK

kill -9 <mydb进程>; faketime -f "+14y" mydb ...      # 2040 下重启
mysql> SELECT COUNT(*) FROM d40.t40;
# 2                                 ← 数据完整恢复
```

**旁白**（约 80 字）：
> "同一时刻，换 MyDB：启动成功；NOW() 返回 2040；TIMESTAMP 默认值正常写入；直接插一条 2060 年的数据——没有任何报错；建用户正常；最后 kill -9 暴力重启，数据一条不少。"

**字幕关键词**：`2040 OK` `2060 直插` `kill -9 重启` `数据零丢失`

### S4 · 原理 15 秒（00:50–01:05，15s）

**画面**：动画对比图。左：MySQL 的 TIMESTAMP = 32 位有符号秒计数，进度条在 `2038-01-19 03:14:07` 处爆掉，数据字典元数据列跟着越界。右：MyDB 内部 64 位微秒，进度条无限延伸。

**旁白**（约 60 字）：
> "根因只有一个：MySQL 的 TIMESTAMP 是 32 位秒计数，2038 年 1 月 19 日封顶，连系统元数据都写不进去。MyDB 内部是 64 位微秒，天生没有这道墙。"

**字幕关键词**：`32 位秒 → 2038` `64 位微秒` `no Y2038 wall`

### S5 · CTA（01:05–01:15，10s）

**画面**：画面分屏回放两边报错/全绿对比 0.5s 快闪 → MyDB LOGO + GitHub 地址。

**旁白**（约 40 字）：
> "2038 不是科幻片，是 12 年后真实的生产事故。开源 MyDB，链接在评论区。提前 14 年，先把它跑起来。"

**字幕关键词**：`2038 = 12 年后` `开源 MyDB`

---

## 4. 录屏执行清单（提词器式，可直接照跑）

按顺序执行，每步录一段素材（素材目录建议 `/tmp/mydb/vid2040/`）：

```bash
# A. MySQL 三连败（假时钟全程）
faketime -f "+14y" mysqld --no-defaults --initialize-insecure --datadir=/tmp/mydb/y2040-mysql
faketime -f "+14y" mysqld --no-defaults --datadir=/tmp/mydb/y2026-ctrl --port=3308 --socket=/tmp/mydb/y2040.sock   # 首启失败
# 先 2026 正常启动过一次后，再 2040 启动，执行：
#   CREATE DATABASE d40;                 → ERROR 3507
#   CREATE TABLE d40.dt(dt DATETIME DEFAULT CURRENT_TIMESTAMP(6));  → ERROR 1067

# B. MyDB 全绿（同一假时钟）
faketime -f "+14y" ./target/release/mydb --host 127.0.0.1 --port 4940
#   SELECT NOW(6); / 建表 / 插 2040 / 插 2060 / CREATE USER / kill -9 / 重启 / SELECT COUNT(*)
```

---

## 5. 口径红线（发布前必读，延续 120s 正片的口径纪律）

1. **版本口径**：实测的是 MySQL 26.7.0（2026-10 本机最新 Homebrew 版），旁白只说"MySQL 最新版"，**不说"8.0 实测"**；8.0 的初始化失败引用 Bug #119318 作为佐证。
2. **CREATE USER 画面禁用**：26.7 的 `mysql.user.password_last_changed` 列行为已变，实测 CREATE USER 反而成功——**不要拍"MySQL 建用户失败"**，MySQL 侧画面只用 initialize 失败、首启 Aborting、ERROR 3507 三段。
3. **不说"MyDB 解决了 Y2038"**：准确说法是"MyDB 内部 64 位微秒时间戳，无 2038 上限（实测 2060 直插）"。MySQL 格式 binlog 兼容层遵循 wire 上限：事件头时间戳 4 字节秒（到 2106），行事件 TIMESTAMP2 超 2106 时**显式报错**（提示走原生 WAL 复制），绝不静默截断——若评论区追问，以此口径回复。
4. **测试环境声明**（放简介或片尾小字）：`macOS arm64 · libfaketime +14y · MySQL 26.7.0 · mydb 1.0.0 · 2026-10-28 实测 · 命令全部开源可复现`。
5. **不贬损过度**：标题与旁白不使用"MySQL 已死/劝退"类措辞，只陈述实测结果；2038 表述为"12 年后的生产事故"，不渲染恐慌。
6. **DATETIME 也有坑**：MySQL 侧实测 `DATETIME DEFAULT CURRENT_TIMESTAMP` 建表报 ERROR 1067，可作为评论区彩蛋——"换 DATETIME 也救不了默认值"。

---

## 6. 30 秒竖版切片建议（抖音/视频号）

| 时间码 | 内容 |
|--------|------|
| 0–4s | Hook：2040 大字 + MySQL 初始化失败报错 |
| 4–12s | S1 设置压缩：`faketime +14y`，date 显示 2040 |
| 12–18s | MySQL ERROR 3507 + `Incorrect datetime value '20401024083951'` 特写 |
| 18–27s | MyDB 全绿四连：启动 / NOW()=2040 / 2060 直插 / 重启恢复 |
| 27–30s | CTA：`2038 = 12 年后` + LOGO |

竖版构图：终端居中占 60% 宽，上下留黑；字幕在下方 1/3；关键报错行红框，MyDB 成功行绿框。
