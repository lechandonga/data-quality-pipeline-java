package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 不合格原因，可定位到具体规则与字段。
 *
 * @param ruleId   触发的规则
 * @param field    触发字段（CROSS_FIELD/COMPOSITE 可为空串）
 * @param actual   导致失败的实际值描述
 * @param expected 规则期望值描述
 */
public record FailureReason(String ruleId, String field, String actual, String expected) {
}
