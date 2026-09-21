package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;

/**
 * 一次批次执行请求。workerThreads 允许调用方覆盖（用于复现性测试）；
 * 为 null 时使用配置默认值。
 */
public record ExecutionRequest(String batchId,
                               String dataset,
                               boolean commit,
                               Integer workerThreads,
                               PipelineProperties propertiesOverride) {

    public static ExecutionRequest of(String batchId, String dataset, boolean commit) {
        return new ExecutionRequest(batchId, dataset, commit, null, null);
    }
}
