package com.github.lechandonga.dataqualitypipelinejava.engine;

/**
 * 批次被拒绝异常（如批次级硬规则未通过、容量拒绝等）。
 */
public class BatchRejectedException extends PipelineException {
    public BatchRejectedException(String reasonCode, String message) {
        super(reasonCode, message);
    }
}
