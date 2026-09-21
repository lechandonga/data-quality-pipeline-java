# 本地数据质量校验流水线（data-quality-pipeline）

一套面向本地运行的数据质量校验流水线，在**规则持续演进、批次并发执行、历史基线不断累积**
的前提下，对输入记录给出**稳定、可解释、可复现**的校验结论。

- 规则声明式定义（JSON），规则非法必须整体拒绝并返回可区分的原因码，绝不静默跳过
- 跨批次基线：统计特征累积合并、分布漂移/突变检测，基线缺失零误报，仅成功提交才更新
- 流式处理：有界队列 + 背压 + 明确的容量/并发边界，超限按 REJECT / ISOLATE 策略处理
- 快照隔离：进行中的批次锁定规则快照，规则更新整体生效，不存在半更新状态
- 失败记录隔离与修复重放：原因可定位到规则与字段，重放不重复计数、不改变已通过结论
- 可复现：同一规则快照 + 同一输入多重集，结论与依据不受执行顺序/并发度影响

## 1. 快速开始

需要 JDK 21+、Maven 3.8+。

```bash
mvn spring-boot:run
# 或打包后运行
mvn package
java -jar target/data-quality-pipeline-java-1.0-SNAPSHOT.jar
```

服务默认监听 8080。也可以不启动服务，直接以类库方式嵌入使用（见第 7 节）。

## 2. 声明式规则语义

规则以 JSON 数组声明（示例见 [`examples/rules.json`](examples/rules.json)），通过
`RuleSetLoader` 解析、`RuleSetManager.publish(...)` 发布。**发布是一次原子操作**：
先对全部规则做校验与编译，任一规则非法则整个规则集被拒绝，已发布版本保持不变。

每条规则结构：

| 字段 | 含义 |
| --- | --- |
| `id` | 规则唯一标识，同一规则集内不可重复 |
| `type` | 规则类型，见下表 |
| `field` | 主字段名，必填 |
| `params` | 类型相关参数 |
| `nullable` | 为 `true` 时，字段为空/空白跳过该规则（默认 `false`） |

### 2.1 规则类型

| 类型 | 参数 | 语义 |
| --- | --- | --- |
| `NOT_NULL` | 无 | 字段缺失或纯空白即失败。`NOT_NULL` 不允许 `nullable=true`（语义冲突） |
| `UNIQUE` | 无 | 批次内取值唯一。首次出现通过，第二次及之后失败；每批次独立跟踪，跨批次不共享 |
| `RANGE` | `min` / `max`（至少其一，可单侧） | 字段必须可解析为有限数值，闭区间 `[min,max]`。边界值恰好通过；非数值单独给出原因 |
| `REGEX` | `pattern`（必须是可编译的 Java 正则） | 全串匹配（`Matcher.matches()`）。模式无法编译则规则非法 |
| `CROSS_FIELD` | `otherField`、`operator` | 跨字段一致性。`operator` ∈ `EQUALS`,`NOT_EQUALS`,`GREATER_THAN`,`LESS_THAN`,`GREATER_EQUAL`,`LESS_EQUAL`；大小比较要求双方都是数值 |
| `COMPOSITE` | `mode`（`ALL`/`ANY`）、`rules`（逗号分隔的子规则 id） | 规则组合复用。`ALL` 收集全部子规则失败；`ANY` 任一通过即整体通过。引用必须存在、不可成环、不可直接引用自身 |

### 2.2 非法规则的原因码

非法定义不会被静默跳过，而是抛出携带原因码的 `InvalidRuleException`（REST 为 422）：

`RULE_DEF_MISSING`、`RULE_ID_MISSING`、`RULE_ID_DUPLICATE`、`RULE_TYPE_MISSING`、
`FIELD_MISSING`、`PARAM_MISSING`、`PARAM_INVALID`、`PATTERN_INVALID`、
`RANGE_INVALID`、`NULLABLE_CONFLICT`、`COMPOSITE_INVALID`、
`COMPOSITE_REF_MISSING`、`COMPOSITE_CYCLE`，以及加载期的
`RULESET_PARSE_ERROR` / `RULESET_IO_ERROR` / `RULESET_EMPTY`。

### 2.3 规则演进的一致性保证

- 每个批次在**开始时固定一份不可变规则快照**（带版本号 `v1,v2,...`），执行全程只读该快照；
- 发布新版本只做一次整体引用替换：进行中的批次继续用旧版本，后续批次使用新版本；
- **重放也锁定该批次当时的规则版本**，规则演进不会改变历史批次的判定依据。

## 3. 基线与提交规则

### 3.1 统计内容

对配置的字段（`pipeline.baseline-fields`，逗号分隔）做流式聚合（Welford 在线算法，
不保存原始值）：累计记录数、非空数、可解析数值数、数值均值、标准差、基数（有界）。
统计只依赖输入多重集，单遍扫描且顺序无关，天然可复现。

### 3.2 漂移检查（确定、零误报）

- **首次运行 / 历史基线缺失**：不产生任何漂移告警（`BaselineChecker` 对空基线直接返回空）；
- 样本量不足（任一侧低于最小样本数）时跳过对应检查；
- `BASELINE_MEAN_SHIFT`：当前均值相对历史均值偏离超过 `drift-z-threshold` 个历史标准差；
  历史标准差为 0（常量分布）而当前均值确实改变时给出 `BASELINE_CONSTANT_BREAK`；
- `BASELINE_NULL_RATIO`：当前缺失率与历史缺失率之差严格大于 `null-ratio-delta`。

漂移记录以合成记录键 `__baseline__<field>` 进入隔离清单，原因可定位到字段与阈值。

### 3.3 提交协议（关键）

只有满足以下全部条件的批次才会更新基线：

1. 输入完整消费，未触发容量 `REJECT`、未被标记为 `ISOLATED`；
2. 消费者未发生致命错误；
3. 失败记录占比 ≤ `pipeline.max-reject-ratio`；
4. **所有字段基线的 load → 合并 → commit 全部成功**。

合并采用 Chan 并行公式把本批统计与历史基线按样本量加权累积（而非覆盖）。
多字段提交遵循“全部预检（不改数据）→ 逐字段原子写入 → 任一字段失败则回滚已写字段”，
杜绝一个批次只更新了部分字段基线的半更新状态。
文件存储使用“写临时文件 + 原子 `move`”：任一字段提交失败都会抛 `BASELINE_COMMIT_FAILED`，
批次标记 `FAILED` 且**此前/此后的基线都不被污染**——失败或被拒绝的批次绝不更新基线。

存储选择：`pipeline.baseline-store=file`（默认，目录 `pipeline.storage-path/baselines`，
重启后持续累积）或 `memory`（进程内，演示/测试用）。

## 4. 容量与并发边界

| 配置项 | 默认 | 含义 |
| --- | --- | --- |
| `pipeline.max-records-per-batch` | 1000000 | 单批次记录硬上限，超限按溢出策略处理 |
| `pipeline.max-concurrent-batches` | 4 | 并发批次信号量（公平），超出排队，不额外占内存 |
| `pipeline.record-queue-capacity` | 1024 | 有界队列容量，满时生产者背压阻塞 |
| `pipeline.offer-timeout-millis` | 5000 | 入队最长等待，超时抛 `CAPACITY_EXCEEDED` |
| `pipeline.unique-tracker-capacity` | 1000000 | 批次内唯一值总槽位，超限失败中止批次 |
| `pipeline.overflow-strategy` | REJECT | `REJECT`：抛 `BatchRejectedException(CAPACITY_EXCEEDED)`；`ISOLATE`：截断处理、批次标记 `ISOLATED`、基线不更新 |
| `pipeline.max-reject-ratio` | 1.0 | 失败占比严格大于阈值则批次 `REJECT_RATIO_EXCEEDED` |
| `pipeline.key-field` | id | 隔离/重放定位键，缺失时回退为序号 `#n` |

处理模型：调用方遍历 `Stream` 逐条投喂（**不全量加载**）→ 有界队列 → 每批次单消费者
顺序求值。消费者发生致命错误时通过队列 `abort()` 协作唤醒阻塞的生产者，双方不会互等死锁；
正常结束时结束哨兵一定可靠投递（队列满也会等待空位），消费者不会永久挂起。

## 5. 失败隔离与重放

- 失败记录按 `(batchId, key)` 进入隔离区，`RecordResult.failures` 中每条原因都带
  **规则 id、规则类型、字段、中文原因**；隔离清单按 key 字典序返回（确定顺序）。
- `QualityPipeline.replay(batchId, fixedRecords)` 只处理仍在隔离区的记录：
  - 修复后通过：移出隔离区，计入本次通过，累计通过数单调增加、不重复计数；
  - 仍失败：覆盖更新该键的原因，可无限次重放而不累加计数；
  - 提交已通过记录或未知键：忽略，不改变任何结论；
  - UNIQUE 在“当前仍隔离的记录集合”内按确定顺序（key 字典序）重判；
  - 重放使用**批次当时的规则快照版本**。

## 6. REST API（本地验证）

启动后：

```bash
# 1) 发布规则集（整体生效；非法返回 422 + reasonCode）
curl -s -X POST localhost:8080/api/rules -H 'Content-Type: application/json' \
  -d '{"rules":[{"id":"nn","type":"NOT_NULL","field":"name"}]}'

# 2) 执行批次（逐条进入有界流水线，不要求调用方一次性持有全部数据）
curl -s -X POST localhost:8080/api/batches -H 'Content-Type: application/json' \
  -d '{"batchId":"b1","records":[{"id":"1","name":"a"},{"id":"2"}]}'

# 3) 查看隔离记录（含规则/字段/原因）
curl -s localhost:8080/api/batches/b1/quarantine

# 4) 修复后重放
curl -s -X POST localhost:8080/api/batches/b1/replay -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"2","name":"b"}]}'
```

HTTP 状态：成功 200；规则非法 422（`reasonCode` 区分原因）；批次被拒绝/未知批次 409
（`CAPACITY_EXCEEDED`、`REJECT_RATIO_EXCEEDED`、`BATCH_NOT_FOUND`、`RULESET_MISSING` 等）；
基线 IO 等内部错误 500（`BASELINE_IO_ERROR`）。

批次结果 `BatchResult` 字段：`batchId, ruleSetVersion, status, totalCount, passedCount,
failedCount, cumulativePassed, rejectReason, rejectedRecords`；`status` ∈
`COMMITTED / REJECTED / ISOLATED / FAILED`。

## 7. 以类库方式使用

```java
RuleSetManager rules = new RuleSetManager();
rules.publish(RuleSetLoader.load(Path.of("examples/rules.json")));

QualityPipeline pipeline = new DefaultQualityPipeline(
        props, rules, new InMemoryBaselineStore(), new InMemoryQuarantineService());

try (Stream<Map<String, String>> input = Files.lines(Paths.get("records.ndjson"))
        .map(line -> new ObjectMapper().readValue(line, MAP_TYPE))) {
    BatchResult result = pipeline.execute("daily-2026-09-21", input);
}
```

## 8. 本地验证方法

```bash
# 全量测试（90 个用例，以边界/异常场景为主）
mvn verify

# 按主题运行
mvn test -Dtest=RuleValidationTest            # 规则非法/冲突/组合环/发布原子性
mvn test -Dtest=RuleEvaluationTest            # 各规则类型边界语义
mvn test -Dtest=RuleSetLoaderTest             # JSON 解析/非法文件原因码
mvn test -Dtest=BaselineTest                  # 缺失零误报/漂移/合并/顺序无关
mvn test -Dtest=BaselinePipelineIntegrationTest# 首次零误报、漂移告警、拒绝不污染基线
mvn test -Dtest=PipelineExecutionTest         # 并发批次、规则快照冻结、可复现
mvn test -Dtest=StreamingCapacityTest         # 超大输入背压、容量拒绝/隔离、并发上限
mvn test -Dtest=QuarantineReplayTest          # 隔离原因、重放不重复计数、版本锁定
mvn test -Dtest=FileBaselineStoreTest         # 原子提交、失败不破坏旧基线
mvn test -Dtest=MultiFieldCommitAtomicityTest # 多字段提交预检+回滚，杜绝半更新
mvn test -Dtest=PipelineControllerTest        # REST 端到端
```

测试覆盖的关键边界场景：规则 id 重复、`min>max`、非法正则、未知运算符、NOT_NULL 与
nullable 冲突、组合引用缺失/成环、发布失败旧版本不变、首次运行零误报、样本不足不告警、
常量分布突变、基线提交失败保持旧值、超大输入（20 万记录 + 容量 4 队列）全量处理、
唯一跟踪槽位耗尽、入队超时、并发 8 批次结论一致、批次进行中规则热更新、
输入乱序结论一致、容量 REJECT/ISOLATE、修复重放/重复重放/重放已通过记录等。

日志在关键节点均带批次标识：批次开始（版本/容量/策略）、记录失败（批次+记录键+规则+字段+原因）、
漂移告警、容量拒绝/隔离原因、基线提交失败、重放通过/仍失败及累计计数，便于审计与复现。

## 9. 主要代码结构

```
model/       规则定义、规则集、校验结论等不可变模型
rule/        规则校验、编译求值、组合解析、注册表（原子发布）、JSON 加载、唯一性跟踪
baseline/    Welford 累加器、漂移检查、基线合并、内存/文件存储（原子提交）
stream/      有界阻塞队列（背压/超时/哨兵/abort 协作）
engine/      流水线引擎、单记录求值器、隔离区、异常与原因码
config/      容量/并发配置、Spring 装配
web/         本地验证 REST 接口
```
