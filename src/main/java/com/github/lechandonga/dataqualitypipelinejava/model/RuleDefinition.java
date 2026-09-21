package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.Map;

/**
 * 声明式规则定义（骨架，仅承载配置）。
 *
 * @param id       规则唯一标识
 * @param type     规则类型
 * @param field    主字段名
 * @param params   类型相关参数（如 min/max/pattern/operator/otherField）
 * @param nullable 字段为空时是否跳过该规则
 */
public record RuleDefinition(
        String id,
        RuleType type,
        String field,
        Map<String, String> params,
        boolean nullable) {
}
