package com.github.lechandonga.dataqualitypipelinejava.engine;

import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;

import java.util.List;
import java.util.Map;

/**
 * 失败记录隔离区存储。
 *
 * <p>每条隔离记录保留可定位到具体规则与字段的失败原因；
 * 记录按 (batchId, recordKey) 唯一存放，重放通过则移除、仍失败则覆盖更新，
 * 因此同一条记录不会重复计数。</p>
 */
public interface QuarantineService {

    /**
     * 放入/覆盖一条隔离记录（幂等于 recordKey）。
     */
    void put(String batchId, RecordResult result);

    /**
     * 取出某批次当前全部隔离记录（确定顺序：按 recordKey 字典序）。
     */
    List<RecordResult> list(String batchId);

    /**
     * 重放通过：从隔离区移除。
     */
    void remove(String batchId, String recordKey);

    /**
     * 隔离区当前记录数。
     */
    int size(String batchId);

    /**
     * 重放输入：recordKey + 修复后的记录。
     */
    record ReplayInput(String recordKey, Map<String, String> record) {
    }
}
