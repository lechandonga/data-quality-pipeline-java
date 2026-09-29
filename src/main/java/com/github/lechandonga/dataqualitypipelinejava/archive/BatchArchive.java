package com.github.lechandonga.dataqualitypipelinejava.archive;

import java.util.Optional;

/**
 * 批次结论存档。
 *
 * 实现必须保证：
 * 1. {@link #save} 原子：要么完整的新快照可见，要么旧快照保持不变，绝不留残档；
 * 2. {@link #load} 遇到损坏存档时抛 {@link ArchiveException}，
 *    不能静默返回空（那会与"从未存在"混淆）；
 * 3. 存档目录不存在/批次缺失时 {@link #load} 返回 empty。
 */
public interface BatchArchive {

    /** 是否真正持久化到进程之外（文件系统）。false 表示纯内存实现。 */
    boolean enabled();

    void save(BatchSnapshot snapshot);

    Optional<BatchSnapshot> load(String batchId);
}
