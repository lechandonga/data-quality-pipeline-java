package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 单条校验失败原因，可定位到具体规则与字段。
 */
public record ValidationFailure(
        String ruleId,
        String ruleType,
        String field,
        String reason) {
}
