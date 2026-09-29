package com.github.lechandonga.dataqualitypipelinejava.archive;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleViolation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 存档中的一条记录：原始输入内容（id、原始顺序、有序原始字段）与执行当时的判定结论。
 */
public record ArchivedRecord(String recordId, long order, Map<String, String> data,
                             RuleViolation violation) {

    public ArchivedRecord {
        data = data == null ? Map.of() : new LinkedHashMap<>(data);
    }

    /** 重建为不可变的领域记录，字段顺序与原始输入一致。 */
    public DataRecord toRecord() {
        return new DataRecord(recordId, order, data);
    }
}
