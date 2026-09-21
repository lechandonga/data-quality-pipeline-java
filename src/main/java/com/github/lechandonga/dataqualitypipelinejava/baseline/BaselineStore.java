package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;

import java.util.Optional;

/**
 * 基线持久化抽象。保存必须是原子的：要么新基线完整可见，要么旧基线保留，
 * 不允许出现部分写入的损坏基线。
 */
public interface BaselineStore {

    Optional<Baseline> load(String dataset);

    /** 原子保存（整体替换）。失败必须抛出 {@link BaselineCommitException}，不得静默。 */
    void save(String dataset, Baseline baseline);
}
