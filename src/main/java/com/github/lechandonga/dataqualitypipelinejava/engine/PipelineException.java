package com.github.lechandonga.dataqualitypipelinejava.engine;

/**
 * 流水线基础异常，携带可区分的原因码。
 */
public class PipelineException extends RuntimeException {
    private final String reasonCode;

    public PipelineException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public PipelineException(String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
