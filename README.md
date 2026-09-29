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
# 全量测试（重点覆盖边界与异常：规则冲突、基线缺失/更新失败、
# 超大流式输入、并发批次、容量边界、隔离重放、跨线程可复现、
# 批次存档重启、内存逐出、落盘失败/存档损坏、导出-重放闭环）
mvn test

# 启动本地服务（默认端口 8080，基线默认仅内存）
mvn spring-boot:run
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

# 4) 修复后重放被隔离的记录（幂等，已通过记录结论不变，不重复计数，不二次更新基线）
printf '%s\n' '{"id":"r2","data":{"id":"k2","name":"b","age":"20","start":"1","end":"2"}}' \
  | curl -s -X POST localhost:8080/api/batches/b-20260921-01/replay \
      -H 'Content-Type: application/x-ndjson' --data-binary @-

# 5) 导出当前仍隔离的记录为修复文件（原样 NDJSON：保留 id、原始顺序与字段）
curl -s localhost:8080/api/batches/b-20260921-01/quarantine/export -o b1.quarantine.ndjson
#    人工/脚本修复字段后，原样回灌即可
curl -s -X POST localhost:8080/api/batches/b-20260921-01/replay \
  -H 'Content-Type: application/x-ndjson' --data-binary @b1.quarantine.ndjson
```

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
| `dqp.archive-dir` | 空（内存） | 配置目录后启用**批次存档**：批次完整结论（状态、各计数、每条记录原始内容与判定依据）原子落盘；重启后可按批次标识查询，规则版本一致时可继续导出/重放。损坏存档显式报错（HTTP 500 `ARCHIVE_CORRUPTED`），不静默当成不存在。 |
| `dqp.max-retained-batches` | 100 | 进程内同时保留的完整批次数量上限。超过后**最旧批次先移出内存**：启用存档时逐出后仍可从磁盘回查/重放；未启用存档时查询返回 **410 Gone**（"曾经存在但已不可用"），与 404（从未存在）严格区分。 |

输入全程通过迭代器/有界队列流式处理，不做全量加载；集成测试中 100,000 条记录
在 `queue-capacity=64` 下完整处理，`overflowIsolatedCount=0`、结论全量一致。

## 5. 批次存档、内存保留与隔离重放闭环

### 开启 vs 未开启存档

| | 配置 `dqp.archive-dir`（开启） | 留空（默认，仅内存） |
|---|---|---|
| 批次结论中的 `persistenceStatus` | 落盘成功为 `PERSISTED`；落盘失败为 `NOT_PERSISTED`（本次结论照常返回，但明确告知未安全落盘） | 恒为 `IN_MEMORY_ONLY` |
| 服务重启后 | 按批次标识查询/导出/重放，结论与执行当时逐条一致 | 结论随进程消失 |
| 超过 `max-retained-batches` 被逐出内存 | 自动从存档回读（透明水合），查询与重放照常 | 查询返回 410 Gone、导出/重放返回 `BATCH_EVICTED` |
| 存档文件损坏 | 查询/导出返回 500 `ARCHIVE_CORRUPTED`，绝不静默当成"不存在" | 不适用 |

落盘为**原子写**（同目录临时文件 + `ATOMIC_MOVE`，文件系统不支持时退化为按目标文件
加锁串行的普通 move）：中途失败/被打断要么保留上一份完整存档、要么完整的新存档可见，
不留半个批次的残档；落盘失败不影响本次执行返回的结论，只把它标记成 `NOT_PERSISTED`。

### 查询的四种结局（`GET /api/batches/{batchId}`）

- `200`：完整结论（内存命中或从存档回读，内容一致）；
- `404`：该批次标识**从未执行过**；
- `410 Gone`：批次确实执行过，但未开启存档且已被内存上限逐出（"曾经存在但已不可用"）；
- `500 ARCHIVE_CORRUPTED`：磁盘存档存在但已损坏（截断/非法 JSON/计数与条目不一致等）。

### 隔离、导出与重放

- 不合格记录进入隔离集，结论保留 `ruleId + field + actual + expected`；
  UNIQUE 失败还会指向首次占用该键的记录。
- **导出**：`GET /api/batches/{batchId}/quarantine/export` 返回当前仍隔离记录的
  NDJSON 修复文件，保留记录标识、**原始输入顺序**与**原始字段内容**（缺失字段保持缺失），
  一个字不改；修复字段值后可直接 POST 回 `/replay`。
- **重放**：`POST /api/batches/{batchId}/replay`，请求体为修复后的 NDJSON。响应：
  - `batch`：整批最新完整结论（字段同执行响应）；
  - `nowPassed`：本次提交记录中因修复转为通过的 id；
  - `stillQuarantined`：重放后全批次仍隔离的 id（含本次没参与提交的）。
- 重放按 `recordId` 用新结论替换旧结论：
  - 输入每条 id 都必须属于"当前仍隔离清单"，或是历次重放修复过的 id（保证幂等）；
    其余 id（原始即通过、从未出现）以及**输入内重复的 id**一律整体拒绝
    （HTTP 422，原因码 `NOT_IN_QUARANTINE`）：**整体不生效**，不允许只重放一部分，
    也不会把陌生记录悄悄当作新记录计入批次，已通过记录结论不受影响；
  - 规则版本与执行当时不一致时拒绝（422 `RULES_VERSION_MISMATCH`）；
  - 已通过记录的结论永不改变、位置不变；唯一性判定包含当前所有已通过记录的键；
  - 同一批修复记录反复重放，结论完全幂等；重放绝不二次更新基线；
  - 重放期间若触发 `REJECT`（硬上限），保留原批次结论，不产生部分重放。
- **两条硬不变量**（均有自动化测试）：
  1. 导出文件一个字不改直接重放，整批结论与导出前**逐条一致**；
  2. 任意修复重放完成后，全批次结论恒等于"拿当前这批记录（含还没修复的）从头完整跑一遍"。

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

## 8. 本地验证

```bash
# 全量测试（73 个用例，含存档重启、内存逐出、落盘失败/存档损坏、导出原样重放、
# 修复重放与整批重跑等价、非隔离记录整体拒绝、并发逐出/查询/重放）
mvn test

# 开启批次存档启动（另开终端用第 1 节的 curl 流程执行批次）
mvn spring-boot:run -Dspring-boot.run.arguments=--dqp.archive-dir=./.dqp-batches

# 验证"重启可查、可继续重放"：
# 1) 执行批次 -> 响应 persistenceStatus=PERSISTED，
#    ./.dqp-batches/<batchId>.batch.json 原子落盘
# 2) 停掉服务再重启（仍指向同一目录）
# 3) curl localhost:8080/api/batches/<batchId>            # 仍是执行当时的完整结论
# 4) curl .../quarantine/export 导出 -> 修复 -> POST /replay
```

新增覆盖集中在
`src/test/java/com/github/lechandonga/dataqualitypipelinejava/tests/BatchArchivePersistenceTest.java`
（重启后查询与重放、内存逐出与回查、落盘失败、存档损坏、导出原样重放一致、
修复重放与整批重跑等价、非隔离清单整体拒绝、并发安全）以及
`PipelineIntegrationTest` 中的导出/422 端到端用例。
