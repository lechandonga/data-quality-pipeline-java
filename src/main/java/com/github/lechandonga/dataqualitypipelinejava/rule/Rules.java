package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.CompositeMode;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 五种基础规则与组合规则的具体实现。
 * 全部实现无状态、线程安全；仅依赖记录内容给出确定性结论。
 */
public final class Rules {

    private Rules() {
    }

    /** 字段非空：字段缺失或为空白均判定不合格。 */
    static final class NotNullRule implements RowRule {
        private final String id;
        private final String field;

        NotNullRule(String id, String field) {
            this.id = id;
            this.field = field;
        }

        @Override public String id() {
            return id;
        }

        @Override public List<FailureReason> evaluate(DataRecord record) {
            String value = record.field(field);
            if (value != null && !value.isBlank()) {
                return List.of();
            }
            return List.of(new FailureReason(id, field, String.valueOf(value),
                    "non-blank value required"));
        }
    }

    /** 数值范围约束：min/max 可单边为空表示不限；非数值取值判不合格。 */
    static final class RangeRule implements RowRule {
        private final String id;
        private final String field;
        private final Double min;
        private final Double max;

        RangeRule(String id, String field, Double min, Double max) {
            this.id = id;
            this.field = field;
            this.min = min;
            this.max = max;
        }

        @Override public String id() {
            return id;
        }

        @Override public List<FailureReason> evaluate(DataRecord record) {
            String raw = record.field(field);
            if (raw == null || raw.isBlank()) {
                return List.of(); // 空值由 NOT_NULL 规则负责，避免规则语义重叠
            }
            double v;
            try {
                v = Double.parseDouble(raw.trim());
            } catch (NumberFormatException e) {
                return List.of(new FailureReason(id, field, raw, "numeric value required"));
            }
            if (min != null && v < min) {
                return List.of(new FailureReason(id, field, raw, ">= " + min));
            }
            if (max != null && v > max) {
                return List.of(new FailureReason(id, field, raw, "<= " + max));
            }
            return List.of();
        }
    }

    /** 格式匹配：正则在编译期已验证，此处只做匹配。 */
    static final class RegexRule implements RowRule {
        private final String id;
        private final String field;
        private final Pattern pattern;

        RegexRule(String id, String field, Pattern pattern) {
            this.id = id;
            this.field = field;
            this.pattern = pattern;
        }

        @Override public String id() {
            return id;
        }

        @Override public List<FailureReason> evaluate(DataRecord record) {
            String raw = record.field(field);
            if (raw == null || raw.isBlank()) {
                return List.of();
            }
            if (pattern.matcher(raw).matches()) {
                return List.of();
            }
            return List.of(new FailureReason(id, field, raw, "matches /" + pattern.pattern() + "/"));
        }
    }

    /** 跨字段一致性：数值比较 EQ/NE/GT/GE/LT/LE；非数值取值判不合格（可区分原因）。 */
    static final class CrossFieldRule implements RowRule {
        private final String id;
        private final String field;
        private final String otherField;
        private final String operator;

        CrossFieldRule(String id, String field, String otherField, String operator) {
            this.id = id;
            this.field = field;
            this.otherField = otherField;
            this.operator = operator;
        }

        @Override public String id() {
            return id;
        }

        @Override public List<FailureReason> evaluate(DataRecord record) {
            String a = record.field(field);
            String b = record.field(otherField);
            if (a == null || a.isBlank() || b == null || b.isBlank()) {
                return List.of();
            }
            double x;
            double y;
            try {
                x = Double.parseDouble(a.trim());
                y = Double.parseDouble(b.trim());
            } catch (NumberFormatException e) {
                return List.of(new FailureReason(id, field, a + "," + b,
                        "numeric operands required for " + operator));
            }
            boolean ok = switch (operator) {
                case "EQ" -> x == y;
                case "NE" -> x != y;
                case "GT" -> x > y;
                case "GE" -> x >= y;
                case "LT" -> x < y;
                case "LE" -> x <= y;
                default -> false;
            };
            if (ok) {
                return List.of();
            }
            return List.of(new FailureReason(id, field + "," + otherField, a + " " + operator + " " + b,
                    field + " " + operator + " " + otherField));
        }
    }

    /**
     * 组合规则：复用已存在的子规则。AND 聚合全部子失败原因；
     * OR 在全部子规则均失败时才失败，并保留全部子原因以便解释。
     */
    static final class CompositeRule implements RowRule {
        private final String id;
        private final CompositeMode mode;
        private final List<RowRule> children;

        CompositeRule(String id, CompositeMode mode, List<RowRule> children) {
            this.id = id;
            this.mode = mode;
            this.children = List.copyOf(children);
        }

        @Override public String id() {
            return id;
        }

        @Override public List<FailureReason> evaluate(DataRecord record) {
            List<FailureReason> all = new ArrayList<>();
            int failures = 0;
            for (RowRule child : children) {
                List<FailureReason> r = child.evaluate(record);
                if (r.isEmpty()) {
                    if (mode == CompositeMode.OR) {
                        return List.of(); // 任一子规则通过，OR 即通过
                    }
                } else {
                    failures++;
                    all.addAll(r); // 聚合所有子失败原因，使每条触发规则都可定位
                }
            }
            if (mode == CompositeMode.OR) {
                // 所有子规则均失败：先报告组合规则自身，再保留全部子原因
                all.add(0, ownReason(record, "all child rules failed"));
                return all;
            }
            if (failures == 0) {
                return List.of();
            }
            all.add(0, ownReason(record, failures + " of " + children.size() + " child rules failed"));
            return all;
        }

        private FailureReason ownReason(DataRecord record, String detail) {
            String childIds = children.stream().map(RowRule::id)
                    .reduce((a, b) -> a + "," + b).orElse("");
            return new FailureReason(id, "", mode + "(" + childIds + ")", detail);
        }
    }

    /** 供编译器解析范围边界。 */
    static Double parseBound(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Double.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a number: " + raw);
        }
    }

    /** 供编译器验证正则。 */
    static Pattern compilePattern(String regex) {
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw e;
        }
    }
}
