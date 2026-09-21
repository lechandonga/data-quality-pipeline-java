package com.github.lechandonga.dataqualitypipelinejava.baseline;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存基线存储，用于本地运行与测试。
 *
 * <p>{@link #commit(Baseline)} 只做一次无条件原子引用替换；
 * 多字段提交失败时通过 {@link #rollback(List)} 把已写字段恢复为提交前的值。</p>
 */
public class InMemoryBaselineStore implements BaselineStore {

    private final ConcurrentHashMap<String, Baseline> baselines = new ConcurrentHashMap<>();

    @Override
    public Optional<Baseline> load(String field) {
        return Optional.ofNullable(baselines.get(field));
    }

    @Override
    public void commit(Baseline next) {
        if (next == null || next.field() == null) {
            throw new IllegalArgumentException("提交的基线不能为空且必须包含字段名");
        }
        baselines.put(next.field(), next);
    }

    @Override
    public void rollback(List<Baseline> previous) {
        for (Baseline old : previous) {
            if (old.batchCount() == 0 && old.totalCount() == 0) {
                baselines.remove(old.field());
            } else {
                baselines.put(old.field(), old);
            }
        }
    }

    public int size() {
        return baselines.size();
    }
}
