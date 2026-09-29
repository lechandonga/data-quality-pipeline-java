package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineCommitException;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BatchStatisticsAccumulator;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveCorruptedException;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveException;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveStore;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchSnapshot;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftFinding;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;
import com.github.lechandonga.dataqualitypipelinejava.model.FieldStats;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.PersistenceStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleViolation;
import com.github.lechandonga.dataqualitypipelinejava.rule.RowRule;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet;
import com.github.lechandonga.dataqualitypipelinejava.rule.UniqueRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * 数据质量校验流水线门面。
 *
 * 确定性保证（同一规则版本 + 同一输入，任何并发度结论一致）：
 * 1. 唯一性在单摄入线程按输入顺序判定：首次出现通过，其后重复隔离；
 * 2. 行规则无状态并行评估，结论只取决于记录内容；
 * 3. 结果按 sourceOrder 排序输出；
 * 4. 在途队列与唯一键集合均有上界：达到上限按 ISOLATE/REJECT 策略处理，
 *    绝不无界占用资源。
 *
 * 基线保证：漂移检测只读取执行前已存在的基线；基线仅在 commit=true 且
 * 批次未被拒绝时原子更新；存储失败时批次不提交、旧基线保持不变。
 *
 * 重放保证：按 recordId 替换原结论，已通过记录结论永不变更、不重复计数；
 * 唯一性判定包含当前已通过记录的键；重放绝不再次更新基线。
 */
@Service
public class PipelineService {

    private static final Logger log = LoggerFactory.getLogger(PipelineService.class);

    private final RuleRegistry ruleRegistry;
    private final BaselineService baselineService;
    private final PipelineProperties properties;
    private final BatchArchiveStore archiveStore;
    private final Semaphore concurrencyLimiter;

    /**
     * 批次结论登记表（进程内有界）。
     * keys 为批次插入顺序：超过上限时最旧的先逐出。所有访问在 {@code registryLock}
     * 下进行，查询/重放只会看到完整的旧结论或完整的新结论，不会读到中间态。
     * pinned > 0 的批次（正在执行/重放/读取）不会被逐出。
     */
    private final Object registryLock = new Object();
    private final Map<String, RunState> runs = new LinkedHashMap<>();
    /** 未开启存档时被逐出的批次 id：查询时显式标记为"曾经存在但已不可用"。 */
    private final Set<String> evictedWithoutArchive = new LinkedHashSet<>();

    @Autowired
    public PipelineService(RuleRegistry ruleRegistry,
                           BaselineService baselineService,
                           PipelineProperties properties,
                           ObjectProvider<BatchArchiveStore> archiveStoreProvider) {
        this.ruleRegistry = ruleRegistry;
        this.baselineService = baselineService;
        this.properties = properties;
        this.archiveStore = archiveStoreProvider.getIfAvailable();
        this.concurrencyLimiter = new Semaphore(Math.max(1, properties.maxConcurrentBatches()));
    }

    public PipelineService(RuleRegistry ruleRegistry,
                           BaselineService baselineService,
                           PipelineProperties properties,
                           BatchArchiveStore archiveStore) {
        this.ruleRegistry = ruleRegistry;
        this.baselineService = baselineService;
        this.properties = properties;
        this.archiveStore = archiveStore;
        this.concurrencyLimiter = new Semaphore(Math.max(1, properties.maxConcurrentBatches()));
    }

    /** 未启用存档时的测试/手动装配入口。 */
    public static PipelineService withoutArchive(RuleRegistry ruleRegistry,
                                                 BaselineService baselineService,
                                                 PipelineProperties properties) {
        return new PipelineService(ruleRegistry, baselineService, properties,
                (BatchArchiveStore) null);
    }

    /** 流式执行一个批次；输入为迭代器，全程不全量加载。 */
    public BatchResult execute(ExecutionRequest request, Iterator<DataRecord> records) {
        PipelineProperties props = request.propertiesOverride() != null
                ? request.propertiesOverride() : properties;
        int workers = request.workerThreads() != null
                ? Math.max(1, request.workerThreads()) : Math.max(1, props.workerThreads());
        RuleSet rules = ruleRegistry.current();

        MDC.put("batchId", request.batchId());
        MDC.put("dataset", String.valueOf(request.dataset()));
        try {
            if (!concurrencyLimiter.tryAcquire()) {
                log.warn("batch rejected: concurrent batch limit {} reached",
                        props.maxConcurrentBatches());
                throw new PipelineCapacityException(request.batchId(),
                        "max concurrent batches reached: " + props.maxConcurrentBatches());
            }
            try {
                RunState state = stream(request.dataset(), rules, props, workers, records, null, null);
                BatchResult result = finalize(request.batchId(), request.dataset(), rules, props,
                        state, request.commit());
                state.result = result;
                return registerFinished(request.batchId(), state, result, request.commit());
            } finally {
                concurrencyLimiter.release();
            }
        } finally {
            MDC.remove("batchId");
            MDC.remove("dataset");
        }
    }

    /**
     * 对已隔离记录执行修复后重放（幂等）。必须使用执行时相同的规则版本；
     * 已通过记录结论不变、不重复计数；重放不会再次更新基线。
     */
    public BatchResult replay(String batchId, Iterator<DataRecord> repairedRecords) {
        return replayWithReport(batchId, repairedRecords).result();
    }

    public Optional<BatchResult> resultOf(String batchId) {
        return switch (lookup(batchId)) {
            case Lookup.Found found -> Optional.of(found.result());
            case Lookup.Evicted ignored -> Optional.empty();
            case Lookup.NotFound ignored -> Optional.empty();
            case Lookup.Corrupt ignored -> Optional.empty();
        };
    }

    // ------------------------------------------------------------------
    // 流式摄入 + 有界并行评估
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // 批次登记表、存档与隔离导出/重放闭环
    // ------------------------------------------------------------------

    /** 查询结论的可用性：找到（含从存档恢复）、已逐出不可用、从未存在、存档损坏。 */
    public sealed interface Lookup permits Lookup.Found, Lookup.Evicted, Lookup.NotFound, Lookup.Corrupt {
        record Found(BatchResult result) implements Lookup {
        }

        record Evicted(String batchId) implements Lookup {
        }

        record NotFound(String batchId) implements Lookup {
        }

        record Corrupt(String batchId, String message) implements Lookup {
        }
    }

    /**
     * 查询批次结论。内存未命中且开启存档时从磁盘完整恢复；存档损坏原样向上抛出，
     * 不静默当成不存在；未开启存档且已被逐出时显式返回 {@link Lookup.Evicted}。
     */
    public Lookup lookup(String batchId) {
        synchronized (registryLock) {
            RunState live = runs.get(batchId);
            if (live != null) {
                return new Lookup.Found(live.result);
            }
            if (archiveStore != null) {
                BatchSnapshot snapshot;
                try {
                    snapshot = archiveStore.load(batchId).orElse(null);
                } catch (BatchArchiveCorruptedException ex) {
                    return new Lookup.Corrupt(batchId, ex.getMessage());
                }
                if (snapshot != null) {
                    RunState restored = toRunState(snapshot);
                    restored.result = snapshot.toResult();
                    runs.put(batchId, restored);
                    evictedWithoutArchive.remove(batchId);
                    evictIfNeeded();
                    return new Lookup.Found(restored.result);
                }
            }
            if (evictedWithoutArchive.contains(batchId)) {
                return new Lookup.Evicted(batchId);
            }
            return new Lookup.NotFound(batchId);
        }
    }

    /**
     * 导出批次当前仍被隔离的记录：保留记录标识、原始顺序与原始字段内容，
     * 可直接作为修复文件回灌重放。
     */
    public List<DataRecord> exportQuarantined(String batchId) {
        RunState state = resolveForAccess(batchId);
        List<DataRecord> out = new ArrayList<>();
        for (ViolationEntry e : orderedEntries(state)) {
            if (!e.violation.passed()) {
                out.add(new DataRecord(e.record.id(), e.order, e.record.data()));
            }
        }
        return out;
    }

    /**
     * 修复重放并返回分类报告：本次因修复转为通过、修复后仍隔离、
     * 不属于该批次隔离清单（整次输入整体拒绝，批次结论不变）。
     */
    public ReplayReport replayWithReport(String batchId, Iterator<DataRecord> repairedRecords) {
        RunState original;
        synchronized (registryLock) {
            original = resolveForAccess(batchId);
            pin(original);
        }
        MDC.put("batchId", batchId);
        try {
            if (original.result != null && original.result.status() == BatchStatus.REJECTED) {
                throw new IllegalStateException("rejected batch cannot be replayed: " + batchId);
            }
            RuleSet rules = ruleRegistry.current();
            if (!rules.version().equals(original.rulesVersion)) {
                throw new IllegalStateException(
                        "rules changed since batch execution (was " + original.rulesVersion
                                + ", now " + rules.version() + "); replay requires the same rules version");
            }
            // 待重放的是"被隔离记录"子集，先完整收集再做整体校验：
            // 任何一条非法都整体不生效，绝不允许只重放一部分
            List<DataRecord> repaired = new ArrayList<>();
            repairedRecords.forEachRemaining(repaired::add);

            Set<String> currentQuarantineIds = new LinkedHashSet<>();
            synchronized (original.entries) {
                for (ViolationEntry e : original.entries) {
                    if (!e.violation.passed()) {
                        currentQuarantineIds.add(e.record.id());
                    }
                }
            }
            Set<String> replayIds = new LinkedHashSet<>();
            List<String> notInList = new ArrayList<>();
            for (DataRecord r : repaired) {
                replayIds.add(r.id());
                if (!currentQuarantineIds.contains(r.id())) {
                    notInList.add(r.id());
                }
            }
            if (!notInList.isEmpty()) {
                throw new BatchReplayRejectedException(batchId, notInList);
            }

            RunState state = stream(original.dataset, rules, original.props,
                    original.workers, repaired.iterator(), original, replayIds);
            if (state.rejected) {
                // 重放触发硬 REJECT：保留原批次结论，不产生部分重放
                log.warn("replay rejected, preserving original result: batchId={}, reason={}",
                        batchId, state.rejectReason);
                return new ReplayReport(original.result, List.of(), List.of(), List.of(), true);
            }
            BatchResult result = finalizeReplay(batchId, rules, original, state);
            state.result = result;

            // 分类以重放后的完整状态为准：
            // repairedPassed = 本次输入 id 中重放后通过的；
            // stillQuarantined = 整批当前仍隔离的全部记录（含本次仍失败与未参与本次的）。
            List<String> repairedPassed = new ArrayList<>();
            List<String> stillQuarantined = new ArrayList<>();
            for (ViolationEntry e : orderedEntries(state)) {
                if (!e.violation.passed()) {
                    stillQuarantined.add(e.record.id());
                } else if (replayIds.contains(e.record.id())) {
                    repairedPassed.add(e.record.id());
                }
            }

            persistAfterReplay(batchId, state);
            synchronized (registryLock) {
                // 同一把锁内替换：读者只会看到完整旧结论或完整新结论
                runs.remove(batchId);
                runs.put(batchId, state);
            }
            return new ReplayReport(state.result, List.copyOf(repairedPassed),
                    List.copyOf(stillQuarantined), List.of(), false);
        } finally {
            synchronized (registryLock) {
                unpin(original);
            }
            MDC.remove("batchId");
        }
    }

    /**
     * 取批次可操作状态：内存命中直接用；否则开启存档时从磁盘恢复；
     * 未存档且已逐出 → 显式 IllegalStateException；从未存在 → IllegalArgumentException。
     */
    private RunState resolveForAccess(String batchId) {
        synchronized (registryLock) {
            RunState live = runs.get(batchId);
            if (live != null) {
                return live;
            }
            if (archiveStore != null) {
                BatchSnapshot snapshot = archiveStore.load(batchId).orElse(null);
                if (snapshot != null) {
                    RunState restored = toRunState(snapshot);
                    restored.result = snapshot.toResult();
                    runs.put(batchId, restored);
                    evictedWithoutArchive.remove(batchId);
                    evictIfNeeded();
                    return restored;
                }
            }
            if (evictedWithoutArchive.contains(batchId)) {
                throw new IllegalStateException(
                        "batch existed but was evicted from memory without an archive: " + batchId);
            }
            throw new IllegalArgumentException("batch not found: " + batchId);
        }
    }

    /**
     * 登记执行完成的批次：先原子存档，再放进有界内存登记表。
     * 落盘失败不影响本次返回结论，只显式标记为 NOT_PERSISTED。
     */
    private BatchResult registerFinished(String batchId, RunState state, BatchResult result,
                                         boolean commit) {
        state.commit = commit;
        PersistenceStatus persistence;
        if (archiveStore == null) {
            persistence = PersistenceStatus.IN_MEMORY;
        } else {
            try {
                archiveStore.save(toSnapshot(batchId, state, result));
                persistence = PersistenceStatus.PERSISTED;
            } catch (BatchArchiveException ex) {
                log.error("batch archive write failed; result kept but NOT persisted: "
                        + "batchId={}, error={}", batchId, ex.getMessage());
                persistence = PersistenceStatus.NOT_PERSISTED;
            }
        }
        BatchResult effective = withPersistence(result, persistence);
        state.result = effective;
        synchronized (registryLock) {
            evictedWithoutArchive.remove(batchId);
            runs.put(batchId, state);
            evictIfNeeded();
        }
        return effective;
    }

    private void persistAfterReplay(String batchId, RunState state) {
        if (archiveStore == null) {
            state.result = withPersistence(state.result, PersistenceStatus.IN_MEMORY);
            return;
        }
        try {
            archiveStore.save(toSnapshot(batchId, state, state.result));
            state.result = withPersistence(state.result, PersistenceStatus.PERSISTED);
        } catch (BatchArchiveException ex) {
            log.error("batch archive rewrite failed after replay; result kept in memory "
                    + "but NOT persisted: batchId={}, error={}", batchId, ex.getMessage());
            state.result = withPersistence(state.result, PersistenceStatus.NOT_PERSISTED);
        }
    }

    /**
     * 超过保留上限时按最旧顺序逐出；pin 中的批次（执行/重放中）跳过，
     * 绝不清理正在使用的状态。未安全落盘的条目留下 tombstone，
     * 查询时显式区分为"曾经存在但已不可用"。
     */
    private void evictIfNeeded() {
        int limit = Math.max(1, properties.maxInMemoryBatches());
        Iterator<Map.Entry<String, RunState>> it = runs.entrySet().iterator();
        while (runs.size() > limit && it.hasNext()) {
            Map.Entry<String, RunState> e = it.next();
            if (e.getValue().pins > 0) {
                continue;
            }
            it.remove();
            boolean persisted = e.getValue().result != null
                    && e.getValue().result.persistenceStatus() == PersistenceStatus.PERSISTED;
            if (!persisted) {
                evictedWithoutArchive.add(e.getKey());
            }
            log.debug("batch evicted from memory: batchId={}, persisted={}",
                    e.getKey(), persisted);
        }
    }

    private void pin(RunState state) {
        state.pins++;
    }

    private void unpin(RunState state) {
        state.pins--;
    }

    private static BatchResult withPersistence(BatchResult r, PersistenceStatus status) {
        return new BatchResult(r.batchId(), r.dataset(), r.status(), r.rulesVersion(),
                r.total(), r.passedCount(), r.quarantinedCount(), r.overflowIsolatedCount(),
                r.violations(), r.driftFindings(), r.baselineUpdated(),
                r.rejectReason(), status);
    }

    private BatchSnapshot toSnapshot(String batchId, RunState state, BatchResult result) {
        List<BatchSnapshot.Entry> entries = new ArrayList<>();
        for (ViolationEntry e : orderedEntries(state)) {
            entries.add(new BatchSnapshot.Entry(e.order, e.record.id(),
                    new LinkedHashMap<>(e.record.data()),
                    e.violation.status(), List.copyOf(e.violation.reasons())));
        }
        return new BatchSnapshot(batchId, state.dataset, result.status(),
                state.rulesVersion, state.workers, state.commit, result.rejectReason(),
                result.driftFindings(), result.baselineUpdated(), List.copyOf(entries));
    }

    private RunState toRunState(BatchSnapshot snapshot) {
        RunState state = new RunState();
        state.dataset = snapshot.dataset();
        state.rulesVersion = snapshot.rulesVersion();
        // 重放沿用执行时的容量/并发口径；存档目录属于部署配置，不参与结论
        state.props = new PipelineProperties(snapshot.workers(),
                properties.maxConcurrentBatches(), properties.queueCapacity(),
                properties.distinctCapacity(), properties.overflowPolicy(),
                properties.meanShiftThreshold(), properties.stdShiftThreshold(),
                null, properties.archiveDir(), properties.maxInMemoryBatches());
        state.workers = snapshot.workers();
        state.commit = snapshot.commit();
        RuleSet rules = ruleRegistry.current();
        long maxOrder = -1;
        for (BatchSnapshot.Entry e : snapshot.entries()) {
            RuleViolation v = new RuleViolation(e.recordId(), e.status(),
                    List.copyOf(e.reasons()));
            DataRecord record = new DataRecord(e.recordId(), e.order(),
                    new LinkedHashMap<>(e.data()));
            state.entries.add(new ViolationEntry(e.order(), record, v));
            maxOrder = Math.max(maxOrder, e.order());
            if (v.passed()) {
                for (UniqueRule u : rules.uniqueRules()) {
                    state.uniqueKeys.computeIfAbsent(u.id(), k -> new HashSet<>())
                            .add(u.keyOf(record));
                    state.firstSeenRecord.computeIfAbsent(u.id(), k -> new HashMap<>())
                            .putIfAbsent(u.keyOf(record), record.id());
                }
            }
        }
        state.nextOrder = maxOrder + 1;
        return state;
    }

    private RunState stream(String dataset, RuleSet rules, PipelineProperties props, int workers,
                            Iterator<DataRecord> input, RunState seed, Set<String> replayIds) {
        RunState state = new RunState();
        state.dataset = dataset;
        state.rulesVersion = rules.version();
        state.props = props;
        state.workers = workers;
        seedState(rules, state, seed, replayIds);

        // 有界工作队列 + 固定 worker：在途记录永不超过 queueCapacity。
        // 摄入线程单线程做唯一性判定（确定性）；worker 只做无状态行规则并行评估。
        int capacity = Math.max(1, props.queueCapacity());
        java.util.concurrent.BlockingQueue<OrderedRecord> queue =
                new java.util.concurrent.ArrayBlockingQueue<>(capacity);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                futures.add(startWorker(pool, rules, queue, state));
            }

            boolean rejected = false;
            String rejectReason = null;
            outer:
            while (input.hasNext()) {
                DataRecord record = input.next();
                long order = record.sourceOrder() >= 0 ? record.sourceOrder() : state.nextOrder++;

                List<FailureReason> immediate = new ArrayList<>();
                boolean capacityReject = checkStateCapacity(rules, record, state, props, immediate);
                if (capacityReject) {
                    rejected = true;
                    rejectReason = "unique-state capacity " + props.distinctCapacity()
                            + " exceeded under REJECT policy";
                    break;
                }
                // 唯一性只在记录未被容量隔离时判定，避免隔离记录抢占唯一键
                if (immediate.isEmpty()) {
                    checkUnique(rules, record, state, immediate);
                }

                if (!immediate.isEmpty()) {
                    addQuarantine(state, order, record, immediate);
                    continue;
                }

                // 队列满时阻塞摄入形成背压：内存有界（在途 <= queueCapacity），
                // 且不会随机隔离合法记录，从而结论与消费速度无关、完全可复现。
                // ISOLATE/REJECT 只作用于唯一键状态容量这类"硬上限"。
                boolean enqueued = false;
                boolean interrupted = false;
                while (!enqueued) {
                    try {
                        queue.put(new OrderedRecord(order, record));
                        enqueued = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            // 每个 worker 一个毒丸：保证已入队记录全部被处理
            for (int w = 0; w < workers; w++) {
                putPoison(queue);
            }
            for (CompletableFuture<Void> f : futures) {
                f.join();
            }

            if (rejected) {
                state.rejected = true;
                state.rejectReason = rejectReason;
                log.warn("batch REJECTED: dataset={}, reason={}", dataset, rejectReason);
            }
            return state;
        } finally {
            pool.shutdownNow();
        }
    }

    private void putPoison(java.util.concurrent.BlockingQueue<OrderedRecord> queue) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    queue.put(OrderedRecord.POISON);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private CompletableFuture<Void> startWorker(ExecutorService pool, RuleSet rules,
                                java.util.concurrent.BlockingQueue<OrderedRecord> queue,
                                RunState state) {
        return CompletableFuture.runAsync(() -> {
            try {
                while (true) {
                    OrderedRecord task = queue.take();
                    if (task == OrderedRecord.POISON) {
                        return;
                    }
                    ViolationEntry ve = evaluateRecord(rules, task.record, task.order);
                    synchronized (state.entries) {
                        state.entries.add(ve);
                    }
                    if (!ve.violation.passed()) {
                        for (FailureReason r : ve.violation.reasons()) {
                            log.info("record quarantined: recordId={}, rule={}, field={}, actual={}",
                                    task.record.id(), r.ruleId(), r.field(), r.actual());
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, pool);
    }

    private void addQuarantine(RunState state, long order, DataRecord record,
                               List<FailureReason> reasons) {
        synchronized (state.entries) {
            state.entries.add(new ViolationEntry(order, record,
                    new RuleViolation(record.id(), RecordStatus.QUARANTINED,
                            List.copyOf(reasons))));
        }
        for (FailureReason r : reasons) {
            log.info("record quarantined: recordId={}, rule={}, field={}, actual={}",
                    record.id(), r.ruleId(), r.field(), r.actual());
        }
    }

    /** 携带输入顺序的工作项。 */
    private record OrderedRecord(long order, DataRecord record) {
        static final OrderedRecord POISON = new OrderedRecord(-1, null);
    }

    /** 唯一性：单摄入线程顺序判定，首次出现通过、重复隔离（确定性，与并发度无关）。 */
    private void checkUnique(RuleSet rules, DataRecord record, RunState state,
                             List<FailureReason> reasons) {
        for (UniqueRule u : rules.uniqueRules()) {
            String key = u.keyOf(record);
            Set<String> seen = state.uniqueKeys.computeIfAbsent(u.id(), k -> new HashSet<>());
            if (!seen.add(key)) {
                String firstId = state.firstSeenRecord
                        .computeIfAbsent(u.id(), k -> new HashMap<>()).get(key);
                reasons.add(FailureReasons.unique(u.id(), String.join(",", u.fields()), key, firstId));
            } else {
                state.firstSeenRecord.computeIfAbsent(u.id(), k -> new HashMap<>())
                        .put(key, record.id());
            }
        }
    }

    /**
     * 唯一键集合容量上界：达到 distinctCapacity 后，未见过的新键无法再确认唯一。
     * ISOLATE：该记录以容量原因隔离；REJECT：返回 true 由调用方整批拒绝。
     * 行为确定且不静默（均有可定位原因/拒绝原因）。
     */
    private boolean checkStateCapacity(RuleSet rules, DataRecord record, RunState state,
                                       PipelineProperties props, List<FailureReason> reasons) {
        for (UniqueRule u : rules.uniqueRules()) {
            Set<String> seen = state.uniqueKeys.get(u.id());
            String key = u.keyOf(record);
            boolean unknown = seen == null || !seen.contains(key);
            long size = seen == null ? 0 : seen.size();
            if (size >= props.distinctCapacity() && unknown) {
                if (props.overflowPolicy() == OverflowPolicy.REJECT) {
                    log.warn("batch rejected: unique-state capacity {} exceeded, rule={}",
                            props.distinctCapacity(), u.id());
                    return true;
                }
                reasons.add(FailureReasons.capacity("unique-state:" + u.id(),
                        props.distinctCapacity()));
            }
        }
        return false;
    }

    private ViolationEntry evaluateRecord(RuleSet rules, DataRecord record, long order) {
        List<FailureReason> reasons = new ArrayList<>();
        for (RowRule rule : rules.rowRules()) {
            reasons.addAll(rule.evaluate(record));
        }
        RuleViolation v = reasons.isEmpty()
                ? new RuleViolation(record.id(), RecordStatus.PASSED, List.of())
                : new RuleViolation(record.id(), RecordStatus.QUARANTINED, List.copyOf(reasons));
        return new ViolationEntry(order, record, v);
    }

    private void seedState(RuleSet rules, RunState state, RunState seed, Set<String> replayIds) {
        if (seed == null) {
            return;
        }
        long maxOrder = -1;
        for (ViolationEntry e : seed.entries) {
            // 只丢弃"当前仍隔离且本次提交了修复"的旧结论；上轮已修复转为通过的记录
            // 不属于隔离清单（重放入口已整体校验），其唯一键必须保留，保证反复重放幂等
            if (replayIds != null && replayIds.contains(e.record.id())
                    && !e.violation.passed()) {
                continue;
            }
            maxOrder = Math.max(maxOrder, e.order);
            state.entries.add(e);
            if (e.violation.passed()) {
                for (UniqueRule u : rules.uniqueRules()) {
                    state.uniqueKeys
                            .computeIfAbsent(u.id(), k -> new HashSet<>())
                            .add(u.keyOf(e.record));
                    state.firstSeenRecord
                            .computeIfAbsent(u.id(), k -> new HashMap<>())
                            .putIfAbsent(u.keyOf(e.record), e.record.id());
                }
            }
        }
        state.nextOrder = maxOrder + 1;
    }

    // ------------------------------------------------------------------
    // 结果归并、漂移检测与提交
    // ------------------------------------------------------------------

    private BatchResult finalize(String batchId, String dataset, RuleSet rules,
                                 PipelineProperties props, RunState state, boolean commit) {
        if (state.rejected) {
            // 被拒绝批次：不统计、不查漂移、不提交，绝不污染基线
            return new BatchResult(batchId, dataset, BatchStatus.REJECTED, rules.version(),
                    0, 0, 0, 0L, List.of(), List.of(), false, state.rejectReason,
                    archiveStore == null ? PersistenceStatus.IN_MEMORY
                            : PersistenceStatus.NOT_PERSISTED);
        }

        List<ViolationEntry> ordered = orderedEntries(state);
        BatchStatisticsAccumulator accumulator =
                new BatchStatisticsAccumulator(props.distinctCapacity());
        for (ViolationEntry e : ordered) {
            if (e.violation.passed()) {
                accumulator.accept(e.record);
            }
        }
        Map<String, FieldStats> batchStats = accumulator.snapshot();
        long totalRecords = accumulator.totalRecords();
        // 基线有而本批次完全没出现的字段：按"本批次全部缺失"补齐，
        // 使缺失率漂移可被检测且合并后字段集合不收缩
        baselineService.current(dataset).ifPresent(b -> {
            for (String field : b.fieldStats().keySet()) {
                batchStats.putIfAbsent(field, new FieldStats(
                        totalRecords, totalRecords, 0.0, 0.0,
                        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0L));
            }
        });

        List<DriftFinding> drift = baselineService.checkDrift(dataset, batchStats,
                props.meanShiftThreshold(), props.stdShiftThreshold());
        logDrift(drift);

        BatchStatus status = BatchStatus.ACCEPTED;
        boolean baselineUpdated = false;
        if (commit) {
            try {
                baselineService.commit(dataset, batchStats);
                status = BatchStatus.COMMITTED;
                baselineUpdated = true;
                log.info("batch COMMITTED: batchId={}, total={}, passed={}, quarantined={}",
                        batchId, ordered.size(), countPassed(ordered),
                        ordered.size() - countPassed(ordered));
            } catch (BaselineCommitException ex) {
                log.error("baseline commit failed; batch NOT committed, old baseline preserved: {}",
                        ex.getMessage());
            }
        } else {
            log.info("batch ACCEPTED (dry-run): batchId={}, total={}, passed={}, quarantined={}",
                    batchId, ordered.size(), countPassed(ordered),
                    ordered.size() - countPassed(ordered));
        }

        return new BatchResult(batchId, dataset, status, rules.version(), ordered.size(),
                countPassed(ordered), ordered.size() - countPassed(ordered),
                countOverflow(ordered), toViolations(ordered), List.copyOf(drift),
                baselineUpdated, null,
                archiveStore == null ? PersistenceStatus.IN_MEMORY
                        : PersistenceStatus.NOT_PERSISTED);
    }

    /**
     * 重放归并：保留原批次的提交状态（重放绝不二次更新基线）；
     * 结论只可能因为修复而变化，已通过记录结论保持不变。
     */
    private BatchResult finalizeReplay(String batchId, RuleSet rules, RunState original,
                                       RunState replayed) {
        if (replayed.rejected) {
            // 重放过程被容量策略拒绝：原结果保持不变，避免部分重放造成结论漂移
            log.warn("replay rejected, preserving original result: batchId={}, reason={}",
                    batchId, replayed.rejectReason);
            return original.result;
        }
        List<ViolationEntry> ordered = orderedEntries(replayed);
        long passed = countPassed(ordered);

        BatchStatus status = original.result != null ? original.result.status() : BatchStatus.ACCEPTED;
        boolean baselineUpdated = original.result != null && original.result.baselineUpdated();
        List<DriftFinding> drift = original.result != null
                ? original.result.driftFindings() : List.of();

        log.info("batch replayed: batchId={}, total={}, passed={}, quarantined={}",
                batchId, ordered.size(), passed, ordered.size() - passed);
        return new BatchResult(batchId, original.dataset, status, rules.version(), ordered.size(),
                passed, ordered.size() - passed, countOverflow(ordered),
                toViolations(ordered), drift, baselineUpdated, null,
                original.result != null ? original.result.persistenceStatus()
                        : PersistenceStatus.IN_MEMORY);
    }

    private List<ViolationEntry> orderedEntries(RunState state) {
        List<ViolationEntry> copy;
        synchronized (state.entries) {
            copy = new ArrayList<>(state.entries);
        }
        // 同一顺序号时按 recordId 决胜，保证排序完全确定
        copy.sort(Comparator.comparingLong((ViolationEntry e) -> e.order)
                .thenComparing(e -> e.record.id()));
        return copy;
    }

    private long countPassed(List<ViolationEntry> entries) {
        return entries.stream().filter(e -> e.violation.passed()).count();
    }

    private long countOverflow(List<ViolationEntry> entries) {
        return entries.stream()
                .filter(e -> e.violation.reasons().stream()
                        .anyMatch(r -> FailureReasons.CAPACITY_RULE.equals(r.ruleId())))
                .count();
    }

    private List<RuleViolation> toViolations(List<ViolationEntry> entries) {
        List<RuleViolation> out = new ArrayList<>();
        for (ViolationEntry e : entries) {
            out.add(e.violation);
        }
        return List.copyOf(out);
    }

    private void logDrift(List<DriftFinding> drift) {
        for (DriftFinding d : drift) {
            if (d.status() == DriftStatus.DRIFTED) {
                log.warn("drift detected: field={}, metric={}, baseline={}, batch={}, detail={}",
                        d.field(), d.metric(), d.baselineValue(), d.batchValue(), d.detail());
            }
        }
    }

    // ------------------------------------------------------------------
    // 内部运行态
    // ------------------------------------------------------------------

    /** 携带顺序、原始数据与结论的内部条目。 */
    private static final class ViolationEntry {
        final long order;
        final DataRecord record;
        final RuleViolation violation;

        ViolationEntry(long order, DataRecord record, RuleViolation violation) {
            this.order = order;
            this.record = record;
            this.violation = violation;
        }
    }

    /** 批次的完整内部状态，支持并发隔离与重放（结果可由状态确定地重建）。 */
    private static final class RunState {
        volatile String dataset;
        volatile String rulesVersion;
        volatile PipelineProperties props;
        volatile int workers;
        volatile boolean commit;
        /** 执行/重放期间的引用计数，> 0 时内存逐出必须跳过该批次。 */
        int pins;
        final List<ViolationEntry> entries = java.util.Collections.synchronizedList(new ArrayList<>());
        final Map<String, Set<String>> uniqueKeys = new ConcurrentHashMap<>();
        final Map<String, Map<String, String>> firstSeenRecord = new ConcurrentHashMap<>();
        long nextOrder;
        boolean rejected;
        String rejectReason;
        volatile BatchResult result;
    }
}
