package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存基线存储（未配置文件目录时的默认实现）。
 * 保存是先构造完整快照再放入引用（引用赋值原子），读端永远看到完整基线。
 * 提供故障注入开关，用于验证"基线更新失败不得污染旧基线、批次不得提交"。
 */
public class InMemoryBaselineStore implements BaselineStore {

    private final Map<String, Baseline> store = new ConcurrentHashMap<>();
    private volatile boolean failNextSave = false;

    @Override
    public Optional<Baseline> load(String dataset) {
        return Optional.ofNullable(store.get(dataset));
    }

    @Override
    public synchronized void save(String dataset, Baseline baseline) {
        if (failNextSave) {
            failNextSave = false;
            throw new BaselineCommitException("injected storage failure for dataset=" + dataset,
                    new java.io.IOException("simulated disk failure"));
        }
        store.put(dataset, baseline);
    }

    /** 测试用：让下一次保存抛出异常。 */
    public void armFailure() {
        this.failNextSave = true;
    }
}
