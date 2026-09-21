package com.github.lechandonga.dataqualitypipelinejava.baseline;

import java.util.List;
import java.util.Optional;

/**
 * 基线存储。
 *
 * <p>提交规则：只有批次成功提交（校验完成且未被拒绝）后才能提交基线。
 * 单字段提交是原子的（临时文件 + 原子替换）；多字段场景由
 * {@link #prepareCommit(Baseline)} 预检、{@link #commit(Baseline)} 正式写入、
 * {@link #rollback(java.util.List)} 回滚共同保证整批字段要么全部生效、要么全部不生效。
 * 失败或被拒绝的批次绝不污染历史基线。</p>
 */
public interface BaselineStore {

    Optional<Baseline> load(String field);

    /**
     * 提交前预检：验证该字段可写入（权限、磁盘等），不得改变任何已有数据。
     * 预检失败将阻止整个批次进入提交流程。
     */
    default void prepareCommit(Baseline next) {
        // 默认实现无预检能力
    }

    /**
     * 将一次成功批次的基线原子合并入库（单字段原子）。
     */
    void commit(Baseline next);

    /**
     * 多字段提交中途失败时回滚已写字段到指定旧值。
     *
     * @param previous 已提交字段提交前的基线快照（按提交顺序）
     */
    default void rollback(List<Baseline> previous) {
        // 默认实现不支持回滚
    }
}
