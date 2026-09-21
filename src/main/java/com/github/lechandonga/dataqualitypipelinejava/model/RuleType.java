package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 支持的声明式规则类型。
 */
public enum RuleType {
    /** 字段非空 */
    NOT_NULL,
    /** 批次内取值唯一（跨记录规则，由引擎统一求值） */
    UNIQUE,
    /** 数值范围约束，参数 min/max，至少其一 */
    RANGE,
    /** 格式匹配，参数 pattern（合法正则） */
    REGEX,
    /** 跨字段一致性，参数 otherField + operator */
    CROSS_FIELD,
    /** 规则组合复用，参数 mode=ALL|ANY，rules=子规则 id 列表 */
    COMPOSITE
}
