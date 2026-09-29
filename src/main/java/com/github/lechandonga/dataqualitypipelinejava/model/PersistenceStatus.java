package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 批次结论的持久化状态。
 *
 * <ul>
 *   <li>{@link #IN_MEMORY_ONLY}：未配置存档目录，结论只存在于当前进程内存；
 *       重启或被内存上限逐出后即不可用。</li>
 *   <li>{@link #PERSISTED}：完整结论已原子写入存档目录，重启后可查询、可重放。</li>
 *   <li>{@link #NOT_PERSISTED}：已配置存档，但本次落盘失败；本次返回的结论本身
 *       仍然有效，但绝不能被误认为已安全落盘。</li>
 * </ul>
 */
public enum PersistenceStatus {
    IN_MEMORY_ONLY,
    PERSISTED,
    NOT_PERSISTED
}
