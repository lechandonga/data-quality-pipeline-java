package com.github.lechandonga.dataqualitypipelinejava.rule;

/**
 * 规则定义非法时抛出。必须携带可区分的原因码，调用方不得静默跳过。
 */
public class InvalidRuleException extends RuntimeException {

    private final String ruleId;
    private final Reason reason;

    public enum Reason {
        MISSING_ID,
        DUPLICATE_ID,
        UNSUPPORTED_TYPE,
        MISSING_FIELD,
        INVALID_RANGE,
        INVALID_REGEX,
        INVALID_OPERATOR,
        UNKNOWN_CHILD,
        RECURSIVE_COMPOSITE,
        MISSING_CHILDREN
    }

    public InvalidRuleException(String ruleId, Reason reason, String message) {
        super(message);
        this.ruleId = ruleId;
        this.reason = reason;
    }

    public String ruleId() {
        return ruleId;
    }

    public Reason reason() {
        return reason;
    }
}
