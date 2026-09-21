package com.github.lechandonga.dataqualitypipelinejava.pipeline;

/**
 * 达到容量上限且策略为 REJECT 时抛出，整批拒绝，不占用更多资源。
 */
public class PipelineCapacityException extends RuntimeException {

    private final String batchId;

    public PipelineCapacityException(String batchId, String message) {
        super(message);
        this.batchId = batchId;
    }

    public String batchId() {
        return batchId;
    }
}
