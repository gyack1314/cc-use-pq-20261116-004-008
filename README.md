# 星地火情事件采集与交接网关

本项目供区域应急数据值守人员处理上游已经筛选的候选火情事件。输入为本地 JSONL 观测包；它不处理遥感影像，也不判断真实火情。仓库演示数据均为合成数据。

## 运行

使用 Python 3.11 或更新版本，无第三方依赖。先在项目根目录运行 `python -m orbitbridge demo --workspace /tmp/orbitbridge-demo`，可看到接收、重复导入、交接确认和来源核对结果。演示会重建指定目录中的 `demo.sqlite3`；请仅使用专门的演示目录。

真实本地包的命令顺序为：

```text
python -m orbitbridge --db work.sqlite3 station-add sat-a "站点 A"
python -m orbitbridge --db work.sqlite3 subscribe north-watch forest-north --severity 3 --confidence 0.7
python -m orbitbridge --db work.sqlite3 ingest packet.jsonl
python -m orbitbridge --db work.sqlite3 handoffs --status pending
python -m orbitbridge --db work.sqlite3 ack 1 "值守员甲"
python -m orbitbridge --db work.sqlite3 trace 1
python -m orbitbridge --db work.sqlite3 verify
```

包第一行为 `kind=packet`、`schema=1`、`station_id`、正整数 `sequence`、带时区 `created_at`、`record_count`、`digest`。后续每行是 `kind=observation`，包含 `id`、带时区 `observed_at`、`region`、`latitude`、`longitude`、`confidence`、1–5 的 `severity` 和 `source`。`source` 可为 `infrared`、`optical`、`thermal`。摘要是每条记录按键排序、紧凑 UTF-8 JSON 表示后追加换行的 SHA-256。可参考 `orbitbridge.protocol.encode_packet` 生成测试包。

同站点序号须连续。相同包重放返回 `duplicate`，相同序号不同内容或跳号进入隔离记录。所有观测和交接项在一个 SQLite 事务中提交；无匹配订阅的观测仍被保存。订阅规则只影响后续提交的包。`verify` 检查数据库、游标、批次数与交接来源。

可用 `inspect-packet` 或 `inspect-dir` 在不改数据库的情况下诊断待收件文件。目录中存在跳号时，先用 `recovery-plan sat-a incoming/` 查看可连续提交的包，再用 `recovery-resume sat-a incoming/` 按站点序号接收。`quarantine-retry <编号> <原文件>` 只允许重检与隔离记录身份一致的包。

清单是 UTF-8 JSON，含 schema、name 和 packets 数组；每个条目含 file、station_id、sequence 和文件 SHA-256。清单与包放在同一目录。`manifest-check <清单>` 预检摘要、包身份和游标，`manifest-run <清单>` 执行可连续提交的条目。执行时再次核对文件摘要。

`quality` 检查跨表不变量，`reconcile` 核对站点和订阅数量。`batch-report <编号>` 与 `day-report YYYY-MM-DD` 分别提供批次及日期统计。`export-handoffs <订阅编号> <新文件路径>` 导出交接来源并返回摘要，`export-check <文件路径> <摘要>` 校验文件。导出不会覆盖已有文件。

## 火点事件聚合与拆分

相邻站点的多条观测可能指向同一火点。系统在原始观测之上维护**事件层**，三条主线各自独立、全程留痕、只追加不删除：

- **原始观测**（`observations`）与**逐观测接收义务**（`handoffs`）永久保留；覆盖关系变化时旧义务标记 `withdrawn` 而非删除。
- **当前事件**（`events` + `event_memberships`）：每条观测收件时先建立单例事件，之后通过确认式聚合/拆分重组。成员的每次进入、离开都有时态记录，`event_lineage` 保存 `created/merged/split/corrected` 血缘。
- **值守待办**（`event_duties` + `duty_observations`）：队列按「事件 × 订阅 × 代际」组织。定位修正、订阅停用/重启用或拆分改变覆盖关系时，旧的未决代际标记 `superseded` 并按当前覆盖重新安排；已确认代际作为责任历史保留。

聚合与拆分采用**提案—确认**两阶段，必须由操作人显式确认才生效：

```text
python -m orbitbridge --db work.sqlite3 events
python -m orbitbridge --db work.sqlite3 propose-merge 3 5 --operator 值守班长 --reason "同一火点"
python -m orbitbridge --db work.sqlite3 proposals
python -m orbitbridge --db work.sqlite3 proposal-confirm 1 --operator 值守班长
python -m orbitbridge --db work.sqlite3 propose-split 3 --group 1 2 --group 3 --operator 值守班长 --reason "两个火点"
python -m orbitbridge --db work.sqlite3 proposal-cancel 2 --operator 值守班长
python -m orbitbridge --db work.sqlite3 event 3
```

确认待办用 `ack-duty <待办编号> <操作人>`。确认只覆盖该代际快照中当时所列的来源；后来并入同一事件的观测若仍有未履行义务，会产生**新一代 pending 待办**，不会被旧确认自动覆盖。快照中 `was_pending=0` 的行表示该来源在本代际建立时已经确认过。

区域订阅以自己**实际覆盖的来源**判断应收事件：待办快照逐观测按订阅区域与阈值过滤，跨区合并后一个组看不到无权接收的来源，另一组的接收义务也不会因合并消失。`correct-observation <观测编号>` 可修正区域或坐标（原值写入 `observation_corrections`）；区域变化会撤销失去覆盖的旧义务、为新覆盖建立新义务并重排待办。

收件重放（同包再次导入）仍返回 `duplicate`，回到观测的**当前事件**，不会重建历史聚合。两条已分别确认的事件再合并时，各自的确认代际与操作人历史完整保留，可在 `event <编号>` 的 `duties` 与血缘中追溯。

`duties` 查看事件待办（可按订阅、事件、状态过滤），`duty <编号>` 查看来源快照，`event-overview` 分别给出原始观测数、当前/已归并事件数、交接义务（pending/acknowledged/withdrawn）及每组未履行义务数。数量核对不靠删除记录压低重复数：`reconcile` 的 `events` 段校验当前事件成员总数恒等于原始观测总数，并报告未路由到任何待办的未履行义务。

测试：`python -m unittest discover -s tests -v`。语法检查：`python -m compileall -q orbitbridge`。
