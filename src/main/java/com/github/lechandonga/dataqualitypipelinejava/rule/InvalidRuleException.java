package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;

/**
 * 非法规则定义异常。规则非法时必须拒绝，不得静默跳过。
 */
public class InvalidRuleException extends PipelineException {
    public InvalidRuleException(String reasonCode, String message) {
        super(reasonCode, message);
    }
}
