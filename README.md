# 本地数据质量校验流水线（data-quality-pipeline-java）

一套本地运行的数据质量校验流水线，面向三个持续变化的现实约束：

- **规则持续演进**：规则以声明式 JSON 定义，非法规则被整体拒绝（原因码可区分）；
  更新先编译后原子替换，要么整体生效，要么完全不生效。
- **批次并发执行**：同一规则集可被多个批次并发执行，结论与并发度、执行顺序无关。
- **历史基线累积**：当前批次统计特征与历史基线对比发现漂移；基线只在批次成功提交后
  原子更新，失败/被拒绝/试运行的批次不会污染已有基线。

## 1. 运行与验证

要求 JDK 21+、Maven 3.9+。

```bash
# 全量测试（70 个用例，重点覆盖边界与异常：规则冲突、基线缺失/更新失败、
# 超大流式输入、并发批次、容量边界、隔离重放、跨线程可复现、
# 跨重启存档查询/重放、内存逐出、落盘失败、存档损坏、导出原样重放、
# 修复重放与整批重跑等价、非隔离清单记录整体拒绝）
mvn test

# 启动本地服务（默认端口 8080，基线默认仅内存）
mvn spring-boot:run

# 启用批次结论存档（重启后历史批次与隔离记录仍可查询/重放）
java -jar target/data-quality-pipeline-java-1.0-SNAPSHOT.jar \
  --dqp.archive-dir=./.dqp-archives --dqp.max-in-memory-batches=100
```

### NDJSON 流式接口

请求体为 **NDJSON**（每行一条记录），流水线逐行读取、逐行校验，不全量加载输入。

```bash
# 1) 整体发布规则集（失败返回 400 + reason，旧规则完全不变）
curl -s -X PUT localhost:8080/api/rules \
  -H 'Content-Type: application/json' \
  -d '{"rules":[
    {"id":"nn-name","type":"NOT_NULL","field":"name"},
    {"id":"r-age","type":"RANGE","field":"age","min":"0","max":"120"},
    {"id":"rx-mail","type":"REGEX","field":"mail","pattern":"\\w+@\\w+\\.\\w+"},
    {"id":"x-end","type":"CROSS_FIELD","field":"end","otherField":"start","operator":"GE"},
    {"id":"u-id","type":"UNIQUE","children":["id"]},
    {"id":"c-ok","type":"COMPOSITE","mode":"AND","children":["nn-name","r-age"]}
  ]}'

# 2) 执行批次（commit=true 才会更新基线；false 为试运行）
printf '%s\n' \
  '{"id":"r1","data":{"id":"k1","name":"a","age":"30","start":"1","end":"2"}}' \
  '{"id":"r2","data":{"id":"k2","name":"","age":"200","start":"9","end":"1"}}' \
  | curl -s -X POST 'localhost:8080/api/batches/b-20260921-01/execute?dataset=people&commit=true&workers=4' \
      -H 'Content-Type: application/x-ndjson' --data-binary @-

# 3) 查询批次结论
curl -s localhost:8080/api/batches/b-20260921-01

# 4) 导出当前仍被隔离的记录（NDJSON 修复文件：记录标识、原始顺序、原始字段一字不改）
curl -s localhost:8080/api/batches/b-20260921-01/quarantine/export -o quarantine.ndjson

# 5) 直接编辑该文件修复后回灌重放（只允许提交隔离清单内的记录；
#    幂等，已通过记录结论不变，不重复计数，不二次更新基线）
curl -s -X POST localhost:8080/api/batches/b-20260921-01/replay \
  -H 'Content-Type: application/x-ndjson' --data-binary @quarantine.ndjson
```

重放响应为分类报告：`result`（重放后的整批完整结论）、`repairedPassed`
（本次因修复转为通过的记录 id）、`stillQuarantined`（当前仍隔离的全部记录 id）、
`notInQuarantineList` 与 `rejected`（输入含非隔离清单记录时整次拒绝、整体不生效）。

## 2. 规则语义

规则是声明式 `RuleDefinition`，编译后成为不可变 `RuleSet`，带内容版本号
（定义的 SHA-256 摘要前缀）。

| type | 关键字段 | 语义 |
|---|---|---|
| `NOT_NULL` | `field` | 字段缺失或为空白（`isBlank`）即隔离。 |
| `UNIQUE` | `children`（复用为字段名列表） | 单字段或多字段组合键在批次内唯一。**首次出现通过，其后所有重复键隔离**；唯一性在单摄入线程按输入顺序判定，因此结论与 worker 数、线程调度无关。重复原因会指出"首次出现的记录 id"。 |
| `RANGE` | `field`,`min`,`max` | 数值闭区间，边界可单边留空表示不限；非数值取值隔离（原因与越界可区分）；空值不判（交给 `NOT_NULL`，避免语义重叠）。 |
| `REGEX` | `field`,`pattern` | `Pattern.matches` 全串匹配；正则在编译期校验。 |
| `CROSS_FIELD` | `field`,`otherField`,`operator` | 跨字段数值一致性，算子为 `EQ/NE/GT/GE/LT/LE`；任一侧非数值/缺失有独立原因。 |
| `COMPOSITE` | `mode`,`children` | 复用其它规则（规则可组合、可复用）。`AND`：所有子规则通过才通过；`OR`：任一子规则通过即通过。失败时原因同时包含**组合规则自身**与所有触发的子规则，保证可解释。 |

失败原因 `FailureReason` 始终带 `ruleId / field / actual / expected`，
可定位到具体规则与字段（容量原因使用固定伪规则 id `__capacity__`）。

### 非法规则被拒绝的原因码（不静默跳过）

`MISSING_ID`、`DUPLICATE_ID`、`UNSUPPORTED_TYPE`、`MISSING_FIELD`、
`INVALID_RANGE`（边界非数值/上下界颠倒/两边界皆空）、`INVALID_REGEX`、
`INVALID_OPERATOR`、`UNKNOWN_CHILD`（含引用 UNIQUE 规则）、
`RECURSIVE_COMPOSITE`（自引用或相互递归）、`MISSING_CHILDREN`。

任何一条非法都会让整次发布失败，**旧规则集保持不变**，不存在半更新状态。

### 运行中更新规则

批次启动时绑定当时的规则快照引用；之后发布的新规则不会影响进行中的批次。
**重放必须使用与原执行相同的规则版本**，否则被显式拒绝（保证可复现）。

## 3. 基线、漂移与提交规则

- **基线内容**：每个数据集（dataset）一份，按字段累积 `count / nullCount /
  mean / variance / min / max / distinct`；数值均值/方差采用 Welford 在线算法，
  跨批次用记录数加权的并行矩公式合并。
- **首次运行 / 基线缺失**：每个字段返回 `NO_BASELINE`，**绝不产出 `DRIFTED` 误报**；
  基线中存在但本批次完全缺失的字段按 100% 缺失参与比较。
- **漂移指标**：
  - `mean-zscore`：`|batchMean - baseMean| / baseStd` 超过 `dqp.mean-shift-threshold`；
  - `null-rate`：字段缺失率绝对差超过 `dqp.std-shift-threshold`。
- **提交规则（关键）**：
  - 只有 `commit=true` 且批次**未被拒绝**时才更新基线；统计仅基于**已通过**记录；
  - 存储原子保存（文件存储为"临时文件 + 原子 move"）；
  - 存储失败时批次保持 `ACCEPTED`（不会标记为 `COMMITTED`），`baselineUpdated=false`，
    **旧基线原样保留**；
  - 试运行（`commit=false`）、被拒绝（`REJECTED`）批次绝不写基线；
  - **重放不产生新的基线修订**，只继承原批次的提交标志。

## 4. 容量与并发边界

| 配置项 | 默认 | 含义与触顶行为 |
|---|---|---|
| `dqp.worker-threads` | 4 | 单批次无状态行规则评估线程数。 |
| `dqp.max-concurrent-batches` | 8 | 同时执行的批次数硬上限，超出直接拒绝（HTTP 429，`CAPACITY_EXCEEDED`）。 |
| `dqp.queue-capacity` | 10000 | 单批次在途记录的有界队列大小。队列满时摄入线程**阻塞背压**，内存始终有界且不会随机隔离合法记录，因此结论与消费速度无关。 |
| `dqp.distinct-capacity` | 1000000 | 唯一键集合 / 字段基数集合的条目上界，防止超大输入无界占用内存。触顶后对无法再确认唯一的**新键**按 `dqp.overflow-policy` 处理。 |
| `dqp.overflow-policy` | ISOLATE | `ISOLATE`：记录隔离并携带可定位的容量原因（`__capacity__`）；`REJECT`：整批拒绝，状态为 `REJECTED`，不统计、不查漂移、不提交。 |
| `dqp.mean-shift-threshold` | 3.0 | 均值漂移的 z-score 阈值。 |
| `dqp.std-shift-threshold` | 0.3 | 缺失率漂移的绝对差阈值。 |
| `dqp.baseline-dir` | 空（内存） | 配置目录后启用文件基线（原子写、跨重启保留）；损坏的基线文件会显式报错而非静默重置。 |
| `dqp.archive-dir` | 空（内存） | 配置目录后启用**批次结论存档**：每批次的状态、各计数、每条记录的原始字段/顺序/判定依据原子落盘；重启后可查询、可重放。损坏存档显式报错（HTTP 500 `ARCHIVE_CORRUPTED`）。 |
| `dqp.max-in-memory-batches` | 100 | 进程内保留的批次结论数量上限，超出后**最旧的先移出内存**。已存档批次逐出后仍可经磁盘回查/重放；未存档批次逐出后查询返回 410（`EVICTED_NOT_AVAILABLE`），与 404"从未存在"区分。 |

输入全程通过迭代器/有界队列流式处理，不做全量加载；集成测试中 100,000 条记录
在 `queue-capacity=64` 下完整处理，`overflowIsolatedCount=0`、结论全量一致。

## 5. 隔离与重放

- 不合格记录进入隔离集，结论保留 `ruleId + field + actual + expected`；
  UNIQUE 失败还会指向首次占用该键的记录。
- **隔离导出**：`GET /api/batches/{batchId}/quarantine/export` 返回当前仍被隔离记录的
  NDJSON 修复文件，记录标识、原始顺序、原始字段内容一字不改；保存后直接编辑、回灌即可。
- **重放准入（整体校验、整体生效）**：重放请求中的**每一条**记录都必须属于该批次
  当前的隔离清单。混入已通过记录、不存在的记录、重复批次的记录都会让整次请求被
  显式拒绝（HTTP 422，`reason=NOT_IN_QUARANTINE_LIST`，并列出越界记录 id），
  **整体不生效**：不重放任何一条、已通过记录结论不变、计数与存档不发生部分改动。
- 重放按 `recordId` 用新结论替换旧结论：
  - 已通过记录的结论永不改变、位置不变；
  - 唯一性判定包含当前所有已通过记录的键，修复后重复计数不会复活；
  - 对**仍隔离**的记录反复重放，结论完全幂等；
  - 重放期间若触发 `REJECT`（硬上限），保留原批次结论，不产生部分重放。
- **两条硬不变量**（由自动化测试固定）：
  1. 导出文件原样不改直接回灌，整批结论与导出前逐条一致；
  2. 任何重放完成后，全批次结论等于"拿当前这批记录（含尚未修复的）从头完整跑一遍"。

## 5.1 批次结论存档与内存逐出

- **未配置 `dqp.archive-dir`（默认，行为明确）**：批次结论只存在于当前进程内存，
  结论中 `persistenceStatus=IN_MEMORY`；服务重启即不可查。
  超过 `dqp.max-in-memory-batches` 后最旧批次被逐出，再查询返回 **HTTP 410**
  （`EVICTED_NOT_AVAILABLE`，"曾经存在但已不可用"），与 **404**（从未存在）明确区分。
- **配置存档目录后**：
  - 每个批次（含被拒绝批次）执行结束时把完整快照——状态、各计数、漂移结论、
    每条记录的原始字段/顺序/通过或隔离及全部原因、规则版本号——写入存档，
    `persistenceStatus=PERSISTED`；
  - 写入采用"完整序列化 → 同目录临时文件 → 原子 move"。中途失败或被打断都不会留下
    半个批次的残档（临时文件会清理）；落盘失败**不影响本次返回的结论**，
    该批次显式标记为 `persistenceStatus=NOT_PERSISTED`，绝不可误读为安全落盘；
  - 磁盘存档为空、JSON 截断、缺关键字段时查询/重放/导出一律显式失败
    （HTTP 500 `ARCHIVE_CORRUPTED`），不会静默当成"批次不存在"；
  - 重启后新实例指向同一目录即可完整查询；对仍有隔离记录的批次，只要**当前规则版本
    与执行时一致**就能继续提交修复重放，版本不一致照旧拒绝；
  - 内存逐出只释放堆内存，不影响磁盘结论：逐出后查询会从存档透明恢复，重放同样可用；
  - 正在执行/重放的批次受引用计数保护，逐出不会清掉使用中的状态；登记表的所有读写
    在同一把锁内完成，查询读到的要么是完整旧结论、要么是完整新结论，没有中间态。
- 存档只保存"执行时事实"（含规则版本号摘要），不保存规则定义本身——这是刻意的：
  重放的可复现性仍由"规则版本必须一致"强制保证。

## 6. 可复现性的具体保证

同一规则版本 + 等价输入，在 worker=1/2/4/8/16、重复执行、多批次并发下，
每条记录的通过/隔离与触发规则集合完全一致（由
`resultsAreIdenticalAcrossWorkerCountsAndRepeatedRuns`、
`sameRulesConcurrentBatchesProduceConsistentIndependentResults` 覆盖）：

1. 唯一性由单摄入线程按输入顺序判定；
2. 行规则为无状态纯函数，并行只改变耗时不改变结论；
3. 在途队列使用背压而非随机丢弃；
4. 结果统一按 `sourceOrder`（其次 recordId）排序输出；
5. 字段映射保序（`LinkedHashMap`），规则集输出按定义顺序。

## 7. 日志

批次执行期间 MDC 带 `batchId / dataset`，关键日志包含批次标识、触发规则与原因：

- 记录隔离：`record quarantined: recordId=..., rule=..., field=..., actual=...`
- 容量隔离：`record isolated due to ...: recordId=...`
- 整批拒绝：`batch REJECTED: dataset=..., reason=...`
- 漂移：`drift detected: field=..., metric=..., baseline=..., batch=..., detail=...`
- 提交结果：`batch COMMITTED / ACCEPTED (dry-run)`；
  基线写入失败：`baseline commit failed; batch NOT committed, old baseline preserved`
- 规则发布：`rules replaced atomically: version=..., rowRules=..., uniqueRules=...`
