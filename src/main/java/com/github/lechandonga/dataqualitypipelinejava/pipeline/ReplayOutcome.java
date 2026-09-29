package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;

import java.util.List;

/**
 * 一次修复重放的结果。除整批最新结论外，明确区分本次输入记录的三种去向：
 *
 * <ul>
 *   <li>{@link #nowPassed}：本次重放后由隔离转为通过的记录 id；</li>
 *   <li>{@link #stillQuarantined}：修复后仍然隔离的记录 id（仍在隔离清单中）；</li>
 *   <li>不属于当前隔离清单的记录 id 在执行前即导致整批拒绝
 *       （{@link ReplayRejectedException}，{@code NOT_IN_QUARANTINE}），
 *       不会走到这里，因此不会悄悄变成新记录。</li>
 * </ul>
 */
public record ReplayOutcome(
        BatchResult batch,
        List<String> nowPassed,
        List<String> stillQuarantined
) {
}
