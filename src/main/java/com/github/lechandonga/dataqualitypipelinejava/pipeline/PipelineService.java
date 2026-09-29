package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineCommitException;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BatchStatisticsAccumulator;
import com.github.lechandonga.dataqualitypipelinejava.archive.ArchivedRecord;
import com.github.lechandonga.dataqualitypipelinejava.archive.ArchiveException;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchive;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

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
 *
 * 持久化与保留：配置 dqp.archive-dir 后，批次完整结论（含每条记录的原始内容与
 * 判定依据）原子落盘，重启后可查询、可继续重放；未配置时行为明确为仅内存。
 * 进程内最多保留 maxRetainedBatches 个批次的完整状态，超出后最旧批次先被逐出：
 * 启用存档时逐出后仍可从存档回查/重放；未启用时查询返回明确的 EVICTED 结局，
 * 既不返回空结论，也不与"从未存在"混淆。
 */
@Service
public class PipelineService {

    private static final Logger log = LoggerFactory.getLogger(PipelineService.class);

    private final RuleRegistry ruleRegistry;
    private final BaselineService baselineService;
    private final BatchArchive archive;
    private final PipelineProperties properties;
    private final Semaphore concurrencyLimiter;

    /** 完整批次状态（内存保留区）。所有结构性变更都在 {@link #registryLock} 下完成。 */
    private final Map<String, RunState> runs = new ConcurrentHashMap<>();
    /**
     * LRU 顺序集合（迭代顺序即最旧→最新）。用 LinkedHashSet 保证每个 id 只出现一次：
     * 逐出只移除真正最旧且未在使用的批次，绝不会因为重复 id 误删正在使用的状态。
     */
    private final Set<String> lruOrder =
            Collections.synchronizedSet(new LinkedHashSet<>());
    /** 确实执行过、但当前不在内存的批次 id（用于未存档时区分 EVICTED 与 UNKNOWN）。 */
    private final Set<String> evictedBatchIds = new HashSet<>();
    private final Object registryLock = new Object();
    /** 正在执行/重放/逐出处理中的批次，不会被内存上限制裁。 */
    private final Set<String> inUse = new HashSet<>();
    /** 每批次重放互斥锁：同批次并发重放串行化，杜绝运行态并发改写与存档写竞争。 */
    private final Map<String, Object> replayLocks = new ConcurrentHashMap<>();

    @Autowired
    public PipelineService(RuleRegistry ruleRegistry,
                           BaselineService baselineService,
                           PipelineProperties properties,
                           BatchArchive archive) {
        this.ruleRegistry = ruleRegistry;
        this.baselineService = baselineService;
        this.archive = archive;
        this.properties = properties;
        this.concurrencyLimiter = new Semaphore(Math.max(1, properties.maxConcurrentBatches()));
    }

    /** 未配置存档目录时的便捷构造（行为明确为仅内存）。 */
    public PipelineService(RuleRegistry ruleRegistry,
                           BaselineService baselineService,
                           PipelineProperties properties) {
        this(ruleRegistry, baselineService, properties,
                new com.github.lechandonga.dataqualitypipelinejava.archive.InMemoryBatchArchive());
    }

    /** 流式执行一个批次；输入为迭代器，全程不全量加载。 */
    public BatchResult execute(ExecutionRequest request, Iterator<DataRecord> records) {
        PipelineProperties props = request.propertiesOverride() != null
                ? request.propertiesOverride() : properties;
        int workers = request.workerThreads() != null
                ? Math.max(1, request.workerThreads()) : Math.max(1, props.workerThreads());
        RuleSet rules = ruleRegistry.current();
        String batchId = request.batchId();

        MDC.put("batchId", batchId);
        MDC.put("dataset", String.valueOf(request.dataset()));
        try {
            if (!concurrencyLimiter.tryAcquire()) {
                log.warn("batch rejected: concurrent batch limit {} reached",
                        props.maxConcurrentBatches());
                throw new PipelineCapacityException(batchId,
                        "max concurrent batches reached: " + props.maxConcurrentBatches());
            }
            try {
                RunState state;
                synchronized (registryLock) {
                    inUse.add(batchId);
                }
                try {
                    state = stream(request.dataset(), rules, props, workers, records, null, null);
                    BatchResult result = finalize(batchId, request.dataset(), rules, props,
                        state, request.commit());
                    state.result = withPersistence(result,
                            archive.enabled() ? PersistenceStatus.NOT_PERSISTED
                                    : PersistenceStatus.IN_MEMORY_ONLY);
                    publishState(batchId, state);
                    state.result = persistAndAnnotate(state);
                } finally {
                    synchronized (registryLock) {
                        inUse.remove(batchId);
                    }
                }
                return state.result;
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
     *
     * 输入的每条记录 id 都必须属于该批次"当前仍隔离"的清单，且不得重复；
     * 否则整批拒绝（NOT_IN_QUARANTINE，整体不生效，不允许部分重放），
     * 也不会把陌生记录悄悄当作新记录计入批次。
     */
    public ReplayOutcome replay(String batchId, Iterator<DataRecord> repairedRecords) {
        Object batchLock = replayLocks.computeIfAbsent(batchId, k -> new Object());
        synchronized (batchLock) {
            RunState original;
            synchronized (registryLock) {
                inUse.add(batchId);
            }
            try {
                original = runs.get(batchId);
                if (original == null) {
                    BatchSnapshot snapshot = loadSnapshot(batchId);
                    original = hydrate(snapshot);
                    synchronized (registryLock) {
                        runs.put(batchId, original);
                        touch(batchId);
                    }
                    log.info("batch hydrated from archive for replay: batchId={}", batchId);
                }
                return doReplay(batchId, repairedRecords, original);
            } finally {
                synchronized (registryLock) {
                    inUse.remove(batchId);
                }
            }
        }
    }

    private ReplayOutcome doReplay(String batchId, Iterator<DataRecord> repairedRecords,
                                   RunState original) {
        RuleSet rules = ruleRegistry.current();
        if (!rules.version().equals(original.rulesVersion)) {
            throw new ReplayRejectedException("RULES_VERSION_MISMATCH",
                    "rules changed since batch execution (was " + original.rulesVersion
                            + ", now " + rules.version()
                            + "); replay requires the same rules version");
        }
        MDC.put("batchId", batchId);
        try {
            // 被拒绝的批次没有完整结论，禁止重放
            if (original.result != null && original.result.status() == BatchStatus.REJECTED) {
                throw new ReplayRejectedException("BATCH_REJECTED",
                        "rejected batch cannot be replayed: " + batchId);
            }
            // 待重放的是"当前仍隔离记录"子集，规模可控；先收集并整体校验：
            // 任何不属于隔离清单 / 重复出现的 id 都导致整批拒绝（整体不生效）。
            List<DataRecord> repaired = new ArrayList<>();
            repairedRecords.forEachRemaining(repaired::add);
            if (repaired.isEmpty()) {
                throw new ReplayRejectedException("EMPTY",
                        "no records supplied for replay: " + batchId);
            }
            Map<String, Long> orderById = new HashMap<>();
            Set<String> currentlyQuarantined = new HashSet<>();
            for (ViolationEntry entry : orderedEntries(original)) {
                orderById.put(entry.record.id(), entry.order);
                if (!entry.violation.passed()) {
                    currentlyQuarantined.add(entry.record.id());
                }
            }
            Set<String> replayIds = new LinkedHashSet<>();
            List<String> foreign = new ArrayList<>();
            for (DataRecord r : repaired) {
                // 允许：当前仍隔离的记录 + 历次重放已修复（可能已转通过）的记录，
                // 后者保证"导出原样重放/反复修复重放"的幂等性；
                // 原始即通过的记录、从未出现的记录、输入内重复 id 一律整体拒绝。
                boolean repairable = currentlyQuarantined.contains(r.id())
                        || original.repairedRecordIds.contains(r.id());
                if (!repairable) {
                    foreign.add(r.id());
                } else if (!replayIds.add(r.id())) {
                    foreign.add(r.id() + " (duplicate in replay input)");
                }
            }
            if (!foreign.isEmpty()) {
                throw new ReplayRejectedException("NOT_IN_QUARANTINE",
                        "records are not in the current quarantine list of batch " + batchId
                                + ": " + foreign + "; the whole replay input is rejected "
                                + "(no partial replay, batch verdict unchanged)");
            }
            // 修复记录按其原始输入顺序参与评估，保证与"整批从头重跑"完全等价。
            List<DataRecord> orderedRepaired = new ArrayList<>(repaired);
            orderedRepaired.sort(Comparator.comparingLong(r -> orderById.get(r.id())));
            orderedRepaired = orderedRepaired.stream()
                    .map(r -> new DataRecord(r.id(), orderById.get(r.id()), r.data()))
                    .toList();

            RunState state = stream(original.dataset, rules, original.props,
                    original.workers, orderedRepaired.iterator(), original, replayIds);
            state.repairedRecordIds.addAll(original.repairedRecordIds);
            state.repairedRecordIds.addAll(replayIds);
            BatchResult replayedResult = finalizeReplay(batchId, rules, original, state);
            state.result = withPersistence(replayedResult, original.result.persistenceStatus());
            synchronized (registryLock) {
                inUse.add(batchId);
            }
            try {
                publishState(batchId, state);
                state.result = persistAndAnnotate(state);
            } finally {
                synchronized (registryLock) {
                    inUse.remove(batchId);
                }
            }
            applyRetention();

            List<RuleViolation> after = state.result.violations();
            // 本次因修复转通过的 = 本次提交记录中现在通过的；
            // 修复后仍隔离的 = 全批次当前仍隔离的记录（含本次没参与提交的）。
            List<String> nowPassed = new ArrayList<>();
            for (String id : replayIds) {
                RuleViolation fresh = after.stream()
                        .filter(v -> v.recordId().equals(id)).findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "replayed record missing from result: " + id));
                if (fresh.passed()) {
                    nowPassed.add(id);
                }
            }
            List<String> stillQuarantined = new ArrayList<>();
            for (RuleViolation v : after) {
                if (!v.passed()) {
                    stillQuarantined.add(v.recordId());
                }
            }
            log.info("replay outcome: batchId={}, nowPassed={}, stillQuarantined={}",
                    batchId, nowPassed, stillQuarantined);
            return new ReplayOutcome(state.result, List.copyOf(nowPassed),
                    List.copyOf(stillQuarantined));
        } finally {
            MDC.remove("batchId");
        }
    }

    /**
     * 查询批次结论：内存命中即返回；未命中时回退存档（启用存档时回读后继续留在内存）；
     * 两者都没有时依据"是否曾经存在"返回 EVICTED 或 UNKNOWN；存档损坏显式报 Corrupted。
     */
    public BatchLookup lookup(String batchId) {
        synchronized (registryLock) {
            RunState s = runs.get(batchId);
            if (s != null && s.result != null) {
                return new BatchLookup.Found(s.result);
            }
        }
        try {
            Optional<BatchSnapshot> snapshot = archive.load(batchId);
            if (snapshot.isPresent()) {
                RunState hydrated = hydrate(snapshot.get());
                synchronized (registryLock) {
                    runs.put(batchId, hydrated);
                    touch(batchId);
                    evictedBatchIds.remove(batchId);
                }
                applyRetention();
                return new BatchLookup.Found(hydrated.result);
            }
        } catch (ArchiveException e) {
            return new BatchLookup.Corrupted(batchId, e.getMessage());
        }
        synchronized (registryLock) {
            if (runs.containsKey(batchId)) {
                return new BatchLookup.Found(runs.get(batchId).result);
            }
            if (evictedBatchIds.contains(batchId)) {
                return new BatchLookup.Evicted(batchId);
            }
            return new BatchLookup.Unknown(batchId);
        }
    }

    /** 兼容旧调用：仅在内存命中时返回。新代码请使用 {@link #lookup}。 */
    public Optional<BatchResult> resultOf(String batchId) {
        RunState s = runs.get(batchId);
        return s == null ? Optional.empty() : Optional.ofNullable(s.result);
    }

    /**
     * 导出批次当前仍隔离的记录为可直接回灌的修复文件内容（NDJSON）。
     * 保留记录标识、原始输入顺序与原始字段内容，一个字不改；
     * 导出文件原样提交到 replay 时，整批结论必须与导出前逐条一致。
     */
    public List<DataRecord> exportQuarantined(String batchId) {
        synchronized (registryLock) {
            inUse.add(batchId);
        }
        try {
            BatchLookup lookup = lookup(batchId);
            if (lookup instanceof BatchLookup.Found) {
                RunState state = runs.get(batchId);
                if (state == null) {
                    // lookup 与读取之间被逐出（极端竞争）：从存档重新水合一次
                    state = hydrate(loadSnapshot(batchId));
                    synchronized (registryLock) {
                        runs.put(batchId, state);
                        touch(batchId);
                    }
                }
                List<DataRecord> out = new ArrayList<>();
                for (ViolationEntry e : orderedEntries(state)) {
                    if (!e.violation.passed()) {
                        out.add(e.record);
                    }
                }
                return List.copyOf(out);
            }
            if (lookup instanceof BatchLookup.Evicted) {
                throw new ReplayRejectedException("BATCH_EVICTED",
                        "batch existed but its in-memory result was evicted and "
                                + "no archive is configured: " + batchId);
            }
            if (lookup instanceof BatchLookup.Corrupted corrupted) {
                throw new ArchiveException("cannot export batch with corrupted archive: "
                        + corrupted.message());
            }
            throw new ReplayRejectedException("BATCH_NOT_FOUND",
                    "batch not found: " + batchId);
        } finally {
            synchronized (registryLock) {
                inUse.remove(batchId);
            }
        }
    }

    // ------------------------------------------------------------------
    // 流式摄入 + 有界并行评估
    // ------------------------------------------------------------------

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
        // 按原始顺序重放"保留条目"（即未被本次修复替换的记录），重建摄入侧唯一键状态。
        // 与执行当时的摄入语义一致：只有最终通过的记录才真正占键
        //（容量隔离/重复隔离的记录没有占键），因此修复记录重新评估唯一性时，
        // 面对的是与"整批从头跑一遍"完全相同的键空间，结论逐记录等价。
        List<ViolationEntry> retained = new ArrayList<>();
        long maxOrder = -1;
        for (ViolationEntry e : orderedEntries(seed)) {
            // 待重放记录的旧结论整体丢弃，由新结论替换（幂等、不重复计数）
            if (replayIds != null && replayIds.contains(e.record.id())) {
                continue;
            }
            maxOrder = Math.max(maxOrder, e.order);
            retained.add(e);
            state.entries.add(e);
        }
        for (ViolationEntry e : retained) {
            if (!e.violation.passed()) {
                continue;
            }
            List<FailureReason> capacity = new ArrayList<>();
            boolean wouldReject = checkStateCapacity(rules, e.record, state,
                    seed.props, capacity);
            if (wouldReject || !capacity.isEmpty()) {
                continue;
            }
            checkUnique(rules, e.record, state, new ArrayList<>());
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
                    PersistenceStatus.IN_MEMORY_ONLY);
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
                baselineUpdated, null, PersistenceStatus.IN_MEMORY_ONLY);
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
                PersistenceStatus.IN_MEMORY_ONLY);
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

    // ------------------------------------------------------------------
    // 注册表、内存上限逐出与存档水合
    // ------------------------------------------------------------------

    /** 写入内存保留区（覆盖同 id 旧批次），把该批次标记为最新。 */
    private void publishState(String batchId, RunState state) {
        synchronized (registryLock) {
            runs.put(batchId, state);
            touch(batchId);
            evictedBatchIds.remove(batchId);
            applyRetention();
        }
    }

    /** 把批次挪到 LRU 队尾（最近使用）。 */
    private void touch(String batchId) {
        synchronized (lruOrder) {
            lruOrder.remove(batchId);
            lruOrder.add(batchId);
        }
    }

    /**
     * 超过 maxRetainedBatches 时从最旧开始逐出；正在执行/重放/查询水合中的批次
     * （inUse）绝不逐出，因此并发访问不会踩到正在使用的状态。
     * 启用存档的批次逐出后仍可从存档回查/重放；未启用的只保留 id 墓碑，
     * 查询时返回明确的 EVICTED 结局。
     */
    private void applyRetention() {
        synchronized (registryLock) {
            while (runs.size() > Math.max(1, properties.maxRetainedBatches())) {
                String oldest = null;
                synchronized (lruOrder) {
                    for (String id : lruOrder) {
                        if (!inUse.contains(id)) {
                            oldest = id;
                            break;
                        }
                    }
                    if (oldest == null) {
                        return;
                    }
                    lruOrder.remove(oldest);
                }
                runs.remove(oldest);
                evictedBatchIds.add(oldest);
                log.info("batch evicted from memory: batchId={}, archived={}",
                        oldest, archive.enabled());
            }
        }
    }

    /** 加载存档；缺失/损坏/不可读按可区分原因显式失败，绝不静默当不存在。 */
    private BatchSnapshot loadSnapshot(String batchId) {
        Optional<BatchSnapshot> snapshot;
        try {
            snapshot = archive.load(batchId);
        } catch (ArchiveException e) {
            throw e;
        }
        if (snapshot.isEmpty()) {
            synchronized (registryLock) {
                if (evictedBatchIds.contains(batchId)) {
                    throw new ReplayRejectedException("BATCH_EVICTED",
                            "batch existed but its result was evicted from memory and "
                                    + "cannot be recovered (archiving disabled): " + batchId);
                }
            }
            throw new ReplayRejectedException("BATCH_NOT_FOUND",
                    "batch not found: " + batchId);
        }
        return snapshot.get();
    }

    /** 从存档快照确定性重建可继续重放的运行态。 */
    private RunState hydrate(BatchSnapshot snapshot) {
        RunState state = new RunState();
        state.dataset = snapshot.result().dataset();
        state.rulesVersion = snapshot.result().rulesVersion();
        state.props = snapshot.replayProps();
        state.workers = snapshot.workerThreads();
        state.result = snapshot.result();
        long maxOrder = -1;
        for (ArchivedRecord archived : snapshot.records()) {
            DataRecord record = archived.toRecord();
            state.entries.add(new ViolationEntry(archived.order(), record, archived.violation()));
            maxOrder = Math.max(maxOrder, archived.order());
        }
        state.nextOrder = maxOrder + 1;
        state.repairedRecordIds.addAll(snapshot.repairedRecordIds());
        // 重建唯一键摄入状态：按原始顺序对全部条目走一遍占键判定，
        // 规则集只需提供 UniqueRule 定义，这里不重新评估行规则（结论已存档）。
        RuleSet rules = ruleRegistry.current();
        for (ViolationEntry e : orderedEntries(state)) {
            if (!e.violation.passed()) {
                continue;
            }
            checkUnique(rules, e.record, state, new ArrayList<>());
        }
        return state;
    }

    /**
     * 原子落盘当前批次并给结论打上持久化标记。
     * 落盘失败不影响本次执行已得到的结论：返回原结论 + NOT_PERSISTED 明确标记，
     * 绝不让人误以为已安全落盘。未启用存档时标记 IN_MEMORY_ONLY。
     */
    private BatchResult persistAndAnnotate(RunState state) {
        BatchResult result = state.result;
        if (!archive.enabled()) {
            BatchResult inMemory = withPersistence(result, PersistenceStatus.IN_MEMORY_ONLY);
            state.result = inMemory;
            return inMemory;
        }
        try {
            archive.save(toSnapshot(state));
            BatchResult persisted = withPersistence(result, PersistenceStatus.PERSISTED);
            state.result = persisted;
            log.info("batch archive persisted: batchId={}, records={}",
                    result.batchId(), result.total());
            return persisted;
        } catch (RuntimeException ex) {
            BatchResult notPersisted = withPersistence(result, PersistenceStatus.NOT_PERSISTED);
            state.result = notPersisted;
            log.error("batch archive persistence FAILED; verdict returned but marked NOT_PERSISTED: "
                    + "batchId={}", result.batchId(), ex);
            return notPersisted;
        }
    }

    private BatchResult withPersistence(BatchResult result, PersistenceStatus status) {
        if (result.persistenceStatus() == status) {
            return result;
        }
        return new BatchResult(result.batchId(), result.dataset(), result.status(),
                result.rulesVersion(), result.total(), result.passedCount(),
                result.quarantinedCount(), result.overflowIsolatedCount(), result.violations(),
                result.driftFindings(), result.baselineUpdated(), result.rejectReason(), status);
    }

    private BatchSnapshot toSnapshot(RunState state) {
        BatchResult persistedResult = withPersistence(state.result, PersistenceStatus.PERSISTED);
        List<ArchivedRecord> records = new ArrayList<>();
        for (ViolationEntry e : orderedEntries(state)) {
            records.add(new ArchivedRecord(e.record.id(), e.order, e.record.data(), e.violation));
        }
        return new BatchSnapshot(persistedResult, records, state.workers,
                state.props.queueCapacity(), state.props.distinctCapacity(), state.props,
                state.repairedRecordIds);
    }

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
        final List<ViolationEntry> entries = java.util.Collections.synchronizedList(new ArrayList<>());
        final Map<String, Set<String>> uniqueKeys = new ConcurrentHashMap<>();
        final Map<String, Map<String, String>> firstSeenRecord = new ConcurrentHashMap<>();
        long nextOrder;
        boolean rejected;
        String rejectReason;
        /** 历次修复重放涉及过的记录 id（即使已转通过也允许再次提交，保证幂等）。 */
        final Set<String> repairedRecordIds = new HashSet<>();
        volatile BatchResult result;
    }
}
