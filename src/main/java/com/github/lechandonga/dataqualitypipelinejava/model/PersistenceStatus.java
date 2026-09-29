package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 批次结论的持久化状态。
 * <ul>
 *   <li>{@link #IN_MEMORY}：未开启存档（未配置 dqp.archive-dir），结论仅存在于当前进程；</li>
 *   <li>{@link #PERSISTED}：结论已原子写入存档目录，重启后可完整查询与重放；</li>
 *   <li>{@link #NOT_PERSISTED}：存档已开启但本次落盘失败，原结论保留但重启后不可查，
 *       不得与 {@link #PERSISTED} 混淆。</li>
 * </ul>
 */
public enum PersistenceStatus {
    IN_MEMORY,
    PERSISTED,
    NOT_PERSISTED
}
