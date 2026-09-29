package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;

/**
 * 按批次标识查询的四种明确结局，调用方必须分别处理：
 *
 * <ul>
 *   <li>{@link Found}：拿到完整结论（来自内存或存档，内容一致）；</li>
 *   <li>{@link Evicted}：该批次确实执行过，但未启用存档且已被内存上限逐出，
 *       结论已不可用——与"从未存在"严格区分；</li>
 *   <li>{@link Unknown}：该标识从未执行过；</li>
 *   <li>{@link Corrupted}：磁盘上存在存档但内容已损坏，显式报错而非静默忽略。</li>
 * </ul>
 */
public sealed interface BatchLookup
        permits BatchLookup.Found, BatchLookup.Evicted, BatchLookup.Unknown, BatchLookup.Corrupted {

    record Found(BatchResult result) implements BatchLookup {
    }

    record Evicted(String batchId) implements BatchLookup {
    }

    record Unknown(String batchId) implements BatchLookup {
    }

    record Corrupted(String batchId, String message) implements BatchLookup {
    }
}
