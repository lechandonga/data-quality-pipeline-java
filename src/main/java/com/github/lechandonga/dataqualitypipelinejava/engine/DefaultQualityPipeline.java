package com.github.lechandonga.dataqualitypipelinejava.engine;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineChecker;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineMerge;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStats;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.StatsAccumulator;
import com.github.lechandonga.dataqualitypipelinejava.config.OverflowStrategy;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleSet;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRuleSet;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import com.github.lechandonga.dataqualitypipelinejava.rule.UniqueTracker;
import com.github.lechandonga.dataqualitypipelinejava.stream.BoundedRecordQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * 默认流水线实现。
 *
 * <p>边界与并发：</p>
 * <ul>
 *   <li>信号量限制同时执行的批次数；超出则排队等待，不额外占用内存；</li>
 *   <li>生产线程（调用方遍历 Stream）与单消费者之间使用固定容量队列背压，
 *       入队超时按 REJECT/ISOLATE 策略处理；</li>
 *   <li>单消费者保证校验结论只取决于输入多重集与规则快照，与并发度无关；</li>
 *   <li>每批次独立的 {@link UniqueTracker} 有容量上限。</li>
 * </ul>
 *
 * <p>提交规则：仅当批次完整处理、未被拒绝时，才先提交基线再返回 COMMITTED；
 * 处理失败/容量拒绝/基线提交失败时一律不更新基线。</p>
 */
public class DefaultQualityPipeline implements QualityPipeline {

    private static final Logger log = LoggerFactory.getLogger(DefaultQualityPipeline.class);

    private final PipelineProperties properties;
    private final RuleSetManager ruleSetManager;
    private final BaselineStore baselineStore;
    private final QuarantineService quarantineService;
    private final Semaphore concurrencyLimit;
    private final Map<String, BatchRun> runs =
            Collections.synchronizedMap(new LinkedHashMap<>());
    private final AtomicLong batchCounter = new AtomicLong();

    public DefaultQualityPipeline(PipelineProperties properties,
                                  RuleSetManager ruleSetManager,
                                  BaselineStore baselineStore,
                                  QuarantineService quarantineService) {
        this.properties = properties;
        this.ruleSetManager = ruleSetManager;
        this.baselineStore = baselineStore;
        this.quarantineService = quarantineService;
        this.concurrencyLimit = new Semaphore(properties.getMaxConcurrentBatches(), true);
    }

    @Override
    public BatchResult execute(String batchId0, Stream<Map<String, String>> records) {
        CompiledRuleSet snapshot = ruleSetManager.current();
        if (snapshot == null) {
            throw new BatchRejectedException("RULESET_MISSING",
                    "尚未发布任何规则集，批次无法执行");
        }
        return execute(batchId0, snapshot.ruleSet(), records);
    }

    @Override
    public BatchResult execute(String batchId0, RuleSet ruleSet,
                               Stream<Map<String, String>> records) {
        CompiledRuleSet snapshot = resolveSnapshot(ruleSet);
        String batchId = batchId0 != null && !batchId0.isBlank()
                ? batchId0
                : "batch-" + batchCounter.incrementAndGet();
        OverflowStrategy overflow = parseOverflow();
        log.info("批次[{}]开始执行, 规则集版本={}, 容量上限={}, 并发上限={}, 溢出策略={}",
                batchId, snapshot.version(),
                properties.getMaxRecordsPerBatch(),
                properties.getMaxConcurrentBatches(), overflow);

        acquireSlot(batchId);
        BoundedRecordQueue queue = new BoundedRecordQueue(
                properties.getRecordQueueCapacity(),
                properties.getOfferTimeoutMillis());
        UniqueTracker uniqueTracker = new UniqueTracker(
                properties.getUniqueTrackerCapacity());
        RecordEvaluator evaluator = new RecordEvaluator(snapshot, uniqueTracker);
        Map<String, StatsAccumulator> stats = createStatsAccumulators();

        ConsumerOutcome outcome = new ConsumerOutcome();
        Thread consumer = new Thread(
                () -> consumeLoop(batchId, queue, evaluator, stats, outcome),
                "dq-consumer-" + batchId);
        consumer.setDaemon(true);
        consumer.start();
        Throwable producerError = null;
        String capacityReason = null;
        long seen = 0;
        try (Stream<Map<String, String>> stream = records) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                seen++;
                if (seen > properties.getMaxRecordsPerBatch()) {
                    if (overflow == OverflowStrategy.REJECT) {
                        capacityReason = "记录数已超过上限 "
                                + properties.getMaxRecordsPerBatch()
                                + "，按 REJECT 策略拒绝批次";
                        break;
                    }
                    // ISOLATE：停止继续投喂，已入队记录处理完后批次标记为 ISOLATED
                    outcome.isolated = true;
                    outcome.truncatedAt = properties.getMaxRecordsPerBatch();
                    log.warn("批次[{}]记录数超过上限 {}，按 ISOLATE 策略截断，基线不更新",
                            batchId, properties.getMaxRecordsPerBatch());
                    break;
                }
                queue.put(iterator.next());
            }
        } catch (CapacityExceededException e) {
            capacityReason = e.getMessage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            producerError = e;
        } catch (RuntimeException e) {
            producerError = e;
        } finally {
            queue.close();
        }

        try {
            consumer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            concurrencyLimit.release();
        }

        if (outcome.consumerError != null) {
            return finishFailed(batchId, snapshot, outcome.consumerError);
        }
        if (capacityReason != null) {
            return finishRejected(batchId, snapshot, "CAPACITY_EXCEEDED",
                    capacityReason, seen, outcome);
        }
        if (producerError != null) {
            return finishFailed(batchId, snapshot, producerError);
        }
        if (outcome.isolated) {
            BatchResult result = buildResult(batchId, snapshot, BatchStatus.ISOLATED,
                    outcome, "BATCH_ISOLATED",
                    "超过单批次容量上限，部分记录被隔离，基线不更新");
            persistRun(batchId, snapshot, result);
            log.warn("批次[{}]被隔离, 处理={}, 通过={}, 失败={}, 原因={}",
                    batchId, result.totalCount(), result.passedCount(),
                    result.failedCount(), result.rejectReason());
            return result;
        }
        return commitBatch(batchId, snapshot, stats, outcome);
    }

    @Override
    public BatchResult replay(String batchId, List<QuarantineService.ReplayInput> fixedRecords) {
        BatchRun run = runs.get(batchId);
        if (run == null) {
            throw new BatchRejectedException("BATCH_NOT_FOUND",
                    "批次不存在或尚未执行过: " + batchId);
        }
        // 重放固定使用批次开始时的规则快照，运行期间规则更新不影响该批次结论
        CompiledRuleSet snapshot = run.snapshot();
        acquireSlot(batchId);
        try {
            // 当前仍在隔离区的记录（确定顺序：recordKey 字典序）
            List<RecordResult> currentQuarantine = quarantineService.list(batchId);
            Map<String, RecordResult> quarantinedByKey = new LinkedHashMap<>();
            for (RecordResult rr : currentQuarantine) {
                quarantinedByKey.put(rr.recordKey(), rr);
            }

            // 重放请求按 recordKey 排序，保证执行顺序无关、结论可复现
            List<QuarantineService.ReplayInput> ordered = new ArrayList<>(fixedRecords);
            ordered.sort(java.util.Comparator.comparing(QuarantineService.ReplayInput::recordKey));

            // 跳过不属于隔离区的重放项（已通过记录结论不变、不重复计数）
            List<ReplayItem> effective = new ArrayList<>();
            int skippedPassed = 0;
            for (QuarantineService.ReplayInput input : ordered) {
                if (!quarantinedByKey.containsKey(input.recordKey())) {
                    skippedPassed++;
                    log.info("批次[{}]记录[{}]不在隔离区（此前已通过），重放忽略，不重复计数",
                            batchId, input.recordKey());
                    continue;
                }
                effective.add(new ReplayItem(input, quarantinedByKey.get(input.recordKey())));
            }

            // 唯一性重判：先播种“本次不重放”的隔离记录原值，再按确定顺序重放
            UniqueTracker tracker = new UniqueTracker(properties.getUniqueTrackerCapacity());
            RecordEvaluator seedingEvaluator = new RecordEvaluator(snapshot, tracker);
            for (RecordResult existing : currentQuarantine) {
                if (existing.source() == null) {
                    continue;
                }
                boolean included = effective.stream()
                        .anyMatch(item -> item.input.recordKey().equals(existing.recordKey()));
                if (!included) {
                    // 仅触发 UNIQUE 观察：走完整求值但忽略其它失败（它们已在隔离原因中）
                    seedUnique(snapshot, tracker, existing.source());
                }
            }
            RecordEvaluator evaluator = new RecordEvaluator(snapshot, tracker);
            List<RecordResult> stillFailing = new ArrayList<>();
            long passedNow = 0;
            for (ReplayItem item : effective) {
                List<ValidationFailure> failures = evaluator.evaluate(item.input.record());
                if (failures.isEmpty()) {
                    quarantineService.remove(batchId, item.input.recordKey());
                    passedNow++;
                    log.info("批次[{}]记录[{}]重放通过，解除隔离",
                            batchId, item.input.recordKey());
                } else {
                    RecordResult rr = new RecordResult(
                            item.input.recordKey(), false, failures,
                            Map.copyOf(item.input.record()));
                    quarantineService.put(batchId, rr);
                    stillFailing.add(rr);
                    log.info("批次[{}]记录[{}]重放仍失败, 触发规则={}, 字段={}",
                            batchId, item.input.recordKey(),
                            failures.stream().map(ValidationFailure::ruleId).toList(),
                            failures.stream().map(ValidationFailure::field).toList());
                }
            }
            long cumulativePassed = run.cumulativePassed() + passedNow;
            BatchResult result = new BatchResult(
                    batchId, snapshot.version(),
                    quarantineService.size(batchId) == 0 ? BatchStatus.COMMITTED : BatchStatus.ISOLATED,
                    fixedRecords.size(),
                    passedNow,
                    stillFailing.size(),
                    cumulativePassed,
                    null,
                    quarantineService.list(batchId));
            runs.put(batchId, new BatchRun(snapshot, cumulativePassed));
            log.info("批次[{}]重放结束, 本次请求={}, 忽略已通过={}, 本次通过={}, 仍隔离={}, 累计通过={}",
                    batchId, fixedRecords.size(), skippedPassed, passedNow,
                    stillFailing.size(), cumulativePassed);
            return result;
        } finally {
            concurrencyLimit.release();
        }
    }

    // ---------------- 内部实现 ----------------

    private void consumeLoop(String batchId,
                             BoundedRecordQueue queue,
                             RecordEvaluator evaluator,
                             Map<String, StatsAccumulator> stats,
                             ConsumerOutcome outcome) {
        long index = 0;
        try {
            Map<String, String> record;
            while ((record = queue.poll()) != null) {
                index++;
                String key = resolveKey(record, index);
                for (Map.Entry<String, StatsAccumulator> entry : stats.entrySet()) {
                    String value = record.get(entry.getKey());
                    if (value == null || value.trim().isEmpty()) {
                        entry.getValue().acceptNull();
                    } else {
                        entry.getValue().acceptValue(value);
                    }
                }
                List<ValidationFailure> failures = evaluator.evaluate(record);
                if (!failures.isEmpty()) {
                    RecordResult rr = new RecordResult(key, false, failures,
                            Map.copyOf(record));
                    quarantineService.put(batchId, rr);
                    outcome.failed++;
                    outcome.rejected.add(rr);
                    log.info("批次[{}]记录[{}]校验失败, 触发规则={}, 字段={}, 原因={}",
                            batchId, key,
                            failures.stream().map(ValidationFailure::ruleId).toList(),
                            failures.stream().map(ValidationFailure::field).toList(),
                            failures.stream().map(ValidationFailure::reason).toList());
                } else {
                    outcome.passed++;
                }
                outcome.processed++;
            }
        } catch (CapacityExceededException e) {
            outcome.consumerError = e;
            queue.abort();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            outcome.consumerError = e;
            queue.abort();
        } catch (RuntimeException e) {
            outcome.consumerError = e;
            queue.abort();
        }
    }

    private BatchResult commitBatch(String batchId,
                                    CompiledRuleSet snapshot,
                                    Map<String, StatsAccumulator> stats,
                                    ConsumerOutcome outcome) {
        // 漂移检查（基线缺失零误报）
        List<RecordResult> extraRejections = new ArrayList<>();
        BaselineChecker checker = new BaselineChecker(
                properties.getDriftZThreshold(), properties.getNullRatioDelta());
        for (Map.Entry<String, StatsAccumulator> entry : stats.entrySet()) {
            String field = entry.getKey();
            BaselineStats currentStats = entry.getValue().snapshot();
            Optional<Baseline> historical = baselineStore.load(field);
            Optional<BaselineStats> historicalStats = historical.map(this::toStats);
            List<ValidationFailure> drift = checker.check(field, currentStats, historicalStats);
            if (!drift.isEmpty()) {
                String driftKey = "__baseline__" + field;
                RecordResult rr = new RecordResult(driftKey, false, drift);
                extraRejections.add(rr);
                log.warn("批次[{}]字段[{}]基线漂移告警, 触发规则={}",
                        batchId, field,
                        drift.stream().map(ValidationFailure::ruleId).toList());
            }
        }
        double rejectRatio = outcome.processed == 0 ? 0.0
                : (double) outcome.failed / outcome.processed;
        if (rejectRatio > properties.getMaxRejectRatio()) {
            return finishRejected(batchId, snapshot, "REJECT_RATIO_EXCEEDED",
                    String.format("失败占比=%.4f 超过阈值=%.4f",
                            rejectRatio, properties.getMaxRejectRatio()),
                    outcome.processed, outcome);
        }

        // 全部前置条件满足后才提交基线：
        // 1) 先对所有字段做预检（不改变任何数据），任一预检失败则完全不进入写入；
        // 2) 逐字段原子提交；3) 中途失败则把已写字段回滚到提交前的值。
        // 保证“所有字段基线要么整体生效，要么完全不生效”。
        java.util.List<Baseline> merged = new ArrayList<>();
        java.util.List<Baseline> previousOfCommitted = new ArrayList<>();
        try {
            for (Map.Entry<String, StatsAccumulator> entry : stats.entrySet()) {
                String field = entry.getKey();
                BaselineStats currentStats = entry.getValue().snapshot();
                Optional<Baseline> historical = baselineStore.load(field);
                Baseline next = BaselineMerge.merge(field, historical, currentStats);
                baselineStore.prepareCommit(next);
                merged.add(next);
            }
            for (Baseline next : merged) {
                previousOfCommitted.add(baselineStore.load(next.field())
                        .orElseGet(() -> absentBaseline(next.field())));
                baselineStore.commit(next);
            }
        } catch (RuntimeException e) {
            try {
                baselineStore.rollback(previousOfCommitted);
            } catch (RuntimeException rollbackError) {
                log.error("批次[{}]基线回滚也失败: {}", batchId, rollbackError.getMessage(),
                        rollbackError);
            }
            log.error("批次[{}]基线提交失败，批次标记为 FAILED，基线已回滚保持不变: {}",
                    batchId, e.getMessage(), e);
            return buildResult(batchId, snapshot, BatchStatus.FAILED,
                    outcome, "BASELINE_COMMIT_FAILED", e.getMessage());
        }

        for (RecordResult rr : extraRejections) {
            quarantineService.put(batchId, rr);
            outcome.failed++;
            outcome.rejected.add(rr);
        }
        BatchResult result = buildResult(batchId, snapshot, BatchStatus.COMMITTED,
                outcome, null, null);
        persistRun(batchId, snapshot, result);
        log.info("批次[{}]提交成功, 处理={}, 通过={}, 失败/隔离={}",
                batchId, result.totalCount(), result.passedCount(), result.failedCount());
        return result;
    }

    private BatchResult finishRejected(String batchId, CompiledRuleSet snapshot,
                                       String reasonCode, String message,
                                       long seen, ConsumerOutcome outcome) {
        log.warn("批次[{}]被拒绝, 原因码={}, 详情={}", batchId, reasonCode, message);
        BatchResult result = new BatchResult(
                batchId, snapshot.version(), BatchStatus.REJECTED,
                outcome == null ? 0 : outcome.processed,
                outcome == null ? 0 : outcome.passed,
                outcome == null ? 0 : outcome.failed,
                outcome == null ? 0 : outcome.passed,
                reasonCode,
                outcome == null ? List.of() : List.copyOf(outcome.rejected));
        persistRun(batchId, snapshot, result);
        throw new BatchRejectedException(reasonCode,
                "批次 " + batchId + " 被拒绝: " + message);
    }

    private BatchResult finishFailed(String batchId, CompiledRuleSet snapshot, Throwable error) {
        log.error("批次[{}]执行失败，基线不更新: {}", batchId, error.getMessage(), error);
        BatchResult result = new BatchResult(
                batchId, snapshot.version(), BatchStatus.FAILED,
                0, 0, quarantineService.size(batchId), 0,
                "BATCH_FAILED", List.copyOf(quarantineService.list(batchId)));
        persistRun(batchId, snapshot, result);
        return result;
    }

    private BatchResult buildResult(String batchId, CompiledRuleSet snapshot,
                                    BatchStatus status, ConsumerOutcome outcome,
                                    String reasonCode, String message) {
        return new BatchResult(
                batchId, snapshot.version(), status,
                outcome.processed, outcome.passed, outcome.failed, outcome.passed,
                reasonCode, List.copyOf(outcome.rejected));
    }

    private void persistRun(String batchId, CompiledRuleSet snapshot, BatchResult result) {
        runs.put(batchId, new BatchRun(snapshot, result.cumulativePassed()));
    }

    private void acquireSlot(String batchId) {
        try {
            concurrencyLimit.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchRejectedException("INTERRUPTED",
                    "批次 " + batchId + " 等待并发许可时被中断");
        }
    }

    private CompiledRuleSet resolveSnapshot(RuleSet ruleSet) {
        CompiledRuleSet current = ruleSetManager.current();
        if (ruleSet == null) {
            if (current == null) {
                throw new BatchRejectedException("RULESET_MISSING", "尚未发布任何规则集");
            }
            return current;
        }
        if (current != null && current.ruleSet() == ruleSet) {
            return current;
        }
        // 按版本查找当前快照；规则更新后旧版本快照不再可解析（批次应持引用执行）
        if (current != null && current.version().equals(ruleSet.version())) {
            return current;
        }
        throw new BatchRejectedException("RULESET_VERSION_STALE",
                "指定的规则集版本 " + ruleSet.version() + " 不是当前已发布版本，"
                        + "进行中的批次应通过 execute(batchId, Stream) 在开始时固定快照");
    }

    private Map<String, StatsAccumulator> createStatsAccumulators() {
        Map<String, StatsAccumulator> map = new LinkedHashMap<>();
        String configured = properties.getBaselineFields();
        if (configured != null && !configured.isBlank()) {
            for (String field : configured.split(",")) {
                String f = field.trim();
                if (!f.isEmpty()) {
                    map.put(f, new StatsAccumulator(properties.getUniqueTrackerCapacity()));
                }
            }
        }
        return map;
    }

    private String resolveKey(Map<String, String> record, long index) {
        String keyField = properties.getKeyField();
        if (keyField != null && !keyField.isBlank()) {
            String value = record.get(keyField);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "#" + index;
    }

    private void seedUnique(CompiledRuleSet snapshot, UniqueTracker tracker,
                            Map<String, String> record) {
        // 只播种 UNIQUE 规则的观察值，避免其它失败影响重放判定
        for (com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRule rule
                : snapshot.orderedRules()) {
            if (rule.type() == RuleType.UNIQUE) {
                String value = record.get(rule.field());
                boolean blank = value == null || value.trim().isEmpty();
                if (rule.definition().nullable() && blank) {
                    continue;
                }
                tracker.observe(rule.definition(), blank ? "" : value);
            }
        }
    }

    private OverflowStrategy parseOverflow() {
        try {
            return OverflowStrategy.valueOf(properties.getOverflowStrategy().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BatchRejectedException("CONFIG_INVALID",
                    "未知的 overflowStrategy: " + properties.getOverflowStrategy());
        }
    }

    private BaselineStats toStats(Baseline baseline) {
        double variance = baseline.numericCount() > 1
                ? baseline.m2() / (baseline.numericCount() - 1)
                : 0.0;
        return new BaselineStats(
                baseline.totalCount(), baseline.nonNullCount(),
                baseline.numericCount(), baseline.mean(), Math.sqrt(Math.max(0.0, variance)),
                baseline.distinctCount());
    }

    /**
     * 字段首次提交（无历史基线）时，用于回滚记录的“不存在”占位；
     * 回滚逻辑对零计数占位基线等价于删除该字段的已写内容（文件存储写回空基线，
     * load 仍可解析；其统计量全为 0，与字段缺失在漂移检查中等价）。
     */
    private Baseline absentBaseline(String field) {
        return new Baseline(field, 0, 0, 0, 0, 0.0, 0.0, 0);
    }

    /**
     * 单批次消费者的可变产出（仅消费者线程写，结束后主线程读，join 提供 happens-before）。
     */
    private static final class ConsumerOutcome {
        long processed;
        long passed;
        long failed;
        boolean isolated;
        long truncatedAt;
        Throwable consumerError;
        final List<RecordResult> rejected = new ArrayList<>();
    }

    /**
     * 已执行批次的最小持久上下文，供重放定位批次当时的规则快照。
     */
    private record BatchRun(CompiledRuleSet snapshot, long cumulativePassed) {
    }

    /**
     * 一次有效重放项：请求 + 隔离区中的旧结论。
     */
    private record ReplayItem(QuarantineService.ReplayInput input, RecordResult previous) {
    }
}
