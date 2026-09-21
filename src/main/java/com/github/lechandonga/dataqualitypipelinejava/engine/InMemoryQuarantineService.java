package com.github.lechandonga.dataqualitypipelinejava.engine;

import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 内存隔离区。按批次维护 recordKey → RecordResult 的有界映射，
 * 容量由引擎在批次记录数上限层面控制（不允许超过 maxRecordsPerBatch）。
 */
public class InMemoryQuarantineService implements QuarantineService {

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, RecordResult>> store =
            new ConcurrentHashMap<>();

    @Override
    public void put(String batchId, RecordResult result) {
        store.computeIfAbsent(batchId, k -> new ConcurrentHashMap<>())
                .put(result.recordKey(), result);
    }

    @Override
    public List<RecordResult> list(String batchId) {
        ConcurrentHashMap<String, RecordResult> records = store.get(batchId);
        if (records == null) {
            return List.of();
        }
        return records.values().stream()
                .sorted(Comparator.comparing(RecordResult::recordKey))
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void remove(String batchId, String recordKey) {
        Map<String, RecordResult> records = store.get(batchId);
        if (records != null) {
            records.remove(recordKey);
        }
    }

    @Override
    public int size(String batchId) {
        ConcurrentHashMap<String, RecordResult> records = store.get(batchId);
        return records == null ? 0 : records.size();
    }
}
