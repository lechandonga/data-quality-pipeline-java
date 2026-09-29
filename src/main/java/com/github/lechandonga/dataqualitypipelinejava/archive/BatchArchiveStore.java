package com.github.lechandonga.dataqualitypipelinejava.archive;

import java.util.Optional;

/**
 * 批次结论存档抽象。
 *
 * <p>写入必须是原子的：要么完整批次快照可见，要么旧快照（含不存在的状态）
 * 保持不变，绝不允许残留半个批次的残档。
 * 读取时若存档存在但内容损坏/截断，必须抛出
 * {@link BatchArchiveCorruptedException}，不得静默返回空。
 */
public interface BatchArchiveStore {

    Optional<BatchSnapshot> load(String batchId);

    /** 原子保存（整体替换）。失败抛出 {@link BatchArchiveException}。 */
    void save(BatchSnapshot snapshot);

    /** 测试与故障模拟用：让下一次保存抛出 {@link BatchArchiveException}。 */
    default void armNextSaveFailure() {
    }
}
