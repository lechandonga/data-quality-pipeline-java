package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.FieldStats;

import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 流式统计累加器：每来一条记录做 O(字段数) 更新，不保留记录本身，
 * 数值均值/方差采用 Welford 在线算法（数值稳定）。
 * distinct 集合受 distinctCapacity 上界约束：达到上界后只保留计数标记，
 * 不再无界增长内存（统计口径固定、行为确定）。
 */
public final class BatchStatisticsAccumulator {

    private final int distinctCapacity;
    private final Map<String, Mutable> fields = new LinkedHashMap<>();
    private long seen;

    public BatchStatisticsAccumulator(int distinctCapacity) {
        this.distinctCapacity = Math.max(1, distinctCapacity);
    }

    /** 纳入一条记录的全部字段值。字段集合以首条记录为准（顺序确定）。 */
    public void accept(DataRecord record) {
        Map<String, String> data = record.data();
        // 新出现的字段：此前 seen 条记录对它而言都是缺失
        for (String name : data.keySet()) {
            fields.computeIfAbsent(name, k -> {
                Mutable m = new Mutable(distinctCapacity);
                m.backfillNulls(seen);
                return m;
            });
        }
        for (Map.Entry<String, String> e : data.entrySet()) {
            fields.get(e.getKey()).accept(e.getValue());
        }
        seen++;
    }

    /** 输出本批次各字段的统计快照（总体方差）。 */
    public Map<String, FieldStats> snapshot() {
        Map<String, FieldStats> out = new LinkedHashMap<>();
        fields.forEach((name, m) -> out.put(name, m.toStats()));
        return out;
    }

    /** 本批次纳入的记录总数。 */
    public long totalRecords() {
        return seen;
    }

    private static final class Mutable {
        private final int distinctCapacity;
        private long count;
        private long nullCount;
        private double mean;
        private double m2;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private long numericCount;
        private final Set<String> distinct = new HashSet<>();
        private boolean distinctCapped;

        Mutable(int distinctCapacity) {
            this.distinctCapacity = distinctCapacity;
        }

        /** 字段首次出现时，把此前 n 条记录全部计为该字段的空值。 */
        void backfillNulls(long n) {
            count += n;
            nullCount += n;
        }

        void accept(String raw) {
            count++;
            if (raw == null) {
                nullCount++;
            } else if (distinct.size() < distinctCapacity) {
                distinct.add(raw);
            } else {
                distinctCapped = true;
            }
            if (raw == null || raw.isBlank()) {
                return;
            }
            double v;
            try {
                v = Double.parseDouble(raw.trim());
            } catch (NumberFormatException e) {
                return;
            }
            numericCount++;
            double delta = v - mean;
            mean += delta / numericCount;
            m2 += delta * (v - mean);
            min = Math.min(min, v);
            max = Math.max(max, v);
        }

        FieldStats toStats() {
            double variance = numericCount > 0 ? m2 / numericCount : 0.0;
            // 达上界时 distinct 为下界（可能有更多未计入的取值），内存不再增长
            long distinctCount = distinctCapped ? -distinct.size() : distinct.size();
            return new FieldStats(count, nullCount,
                    numericCount > 0 ? mean : 0.0, variance,
                    numericCount > 0 ? min : Double.POSITIVE_INFINITY,
                    numericCount > 0 ? max : Double.NEGATIVE_INFINITY,
                    distinctCount);
        }
    }
}
