package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;

import java.util.List;
import java.util.StringJoiner;

/**
 * UNIQUE 规则描述符。唯一性需要跨记录状态，由引擎在单点
 * （摄入线程）确定性判定，避免并发下"谁先到"的竞态：
 * 第一次出现的键通过，其后所有重复键隔离，与线程调度无关。
 */
public record UniqueRule(String id, List<String> fields) {

    private static final String NULL_MARKER = "<null>";

    public UniqueRule {
        fields = List.copyOf(fields);
    }

    /** 拼接记录在该唯一键上的取值（标记空值，避免拼接歧义）。 */
    public String keyOf(DataRecord record) {
        StringJoiner joiner = new StringJoiner("|");
        for (String f : fields) {
            String v = record.field(f);
            joiner.add(v == null ? NULL_MARKER : v);
        }
        return id + "#" + joiner;
    }
}
