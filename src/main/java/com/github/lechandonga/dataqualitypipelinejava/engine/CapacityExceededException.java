package com.github.lechandonga.dataqualitypipelinejava.engine;

/**
 * 容量超限异常（隔离/拒绝策略为 REJECT 时抛出）。
 */
public class CapacityExceededException extends PipelineException {
    public CapacityExceededException(String message) {
        super("CAPACITY_EXCEEDED", message);
    }
}
