package com.github.lechandonga.dataqualitypipelinejava.model;

/** 声明式规则类型。 */
public enum RuleType {
    NOT_NULL,
    UNIQUE,
    RANGE,
    REGEX,
    CROSS_FIELD,
    COMPOSITE
}
