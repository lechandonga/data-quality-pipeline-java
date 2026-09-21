package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;

/**
 * 声明式规则定义（JSON/YAML 可直接映射）。
 *
 * @param id          规则唯一标识
 * @param type        规则类型
 * @param field       目标字段（COMPOSITE 可空）
 * @param min         RANGE 下界（含），null 表示不限
 * @param max         RANGE 上界（含），null 表示不限
 * @param pattern     REGEX 正则表达式
 * @param otherField  CROSS_FIELD 的对照字段
 * @param operator    跨字段一致性算子（EQ/NE/GT/GE/LT/LE）
 * @param mode        COMPOSITE 的组合模式
 * @param children    COMPOSITE 引用的子规则 id（可组合复用）
 * @param description 人类可读描述
 */
public record RuleDefinition(
        String id,
        RuleType type,
        String field,
        String min,
        String max,
        String pattern,
        String otherField,
        String operator,
        CompositeMode mode,
        List<String> children,
        String description
) {

    /** 便于 JSON 反序列化与测试构造的全参兼容静态工厂（字段顺序同上）。 */
    public static RuleDefinition of(String id, RuleType type, String field, String min, String max,
                                    String pattern, String otherField, String operator,
                                    CompositeMode mode, List<String> children, String description) {
        return new RuleDefinition(id, type, field, min, max, pattern, otherField, operator,
                mode, children, description);
    }
}
