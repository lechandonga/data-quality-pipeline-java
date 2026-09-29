package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;

import java.util.List;

/**
 * 一次修复重放的结果。
 *
 * @param result              重放后的整批完整结论
 * @param repairedPassed      本次因修复而转为通过的记录 id（按重放输入顺序）
 * @param stillQuarantined    修复后仍然隔离的记录 id（按重放输入顺序）
 * @param notInQuarantineList 不属于该批次当前隔离清单、被拒绝的记录 id
 * @param rejected            是否因非法记录被整体拒绝（此时批次结论保持不变）
 */
public record ReplayReport(
        BatchResult result,
        List<String> repairedPassed,
        List<String> stillQuarantined,
        List<String> notInQuarantineList,
        boolean rejected
) {
}
