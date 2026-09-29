package com.github.lechandonga.dataqualitypipelinejava.archive;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 未配置存档目录时使用：存档同样不可跨进程保留，行为明确为"仅内存"。
 * 此时批次被内存上限逐出后即真正不可用（查询返回 EVICTED 而非空结论）。
 */
public class InMemoryBatchArchive implements BatchArchive {

    private final ConcurrentMap<String, BatchSnapshot> snapshots = new ConcurrentHashMap<>();

    @Override
    public boolean enabled() {
        return false;
    }

    @Override
    public void save(BatchSnapshot snapshot) {
        snapshots.put(snapshot.batchId(), snapshot);
    }

    @Override
    public Optional<BatchSnapshot> load(String batchId) {
        return Optional.ofNullable(snapshots.get(batchId));
    }
}
