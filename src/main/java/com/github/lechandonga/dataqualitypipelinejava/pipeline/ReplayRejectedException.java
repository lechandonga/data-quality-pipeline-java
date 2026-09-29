package com.github.lechandonga.dataqualitypipelinejava.pipeline;

/**
 * 修复重放被显式拒绝（可区分的原因码）：
 * <ul>
 *   <li>{@code BATCH_NOT_FOUND}：批次标识从未执行过；</li>
 *   <li>{@code BATCH_EVICTED}：批次执行过但未存档、已被内存上限逐出；</li>
 *   <li>{@code RULES_VERSION_MISMATCH}：当前规则版本与执行时不一致；</li>
 *   <li>{@code BATCH_REJECTED}：原批次是整批拒绝的，没有可重放结论；</li>
 *   <li>{@code NOT_IN_QUARANTINE}：提交记录里有不属于当前隔离清单的记录 id，
 *       或同一 id 在输入中重复；该输入整体不生效，不允许部分重放；</li>
 *   <li>{@code EMPTY}：没有提交任何记录。</li>
 * </ul>
 */
public class ReplayRejectedException extends RuntimeException {

    private final String reason;

    public ReplayRejectedException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
