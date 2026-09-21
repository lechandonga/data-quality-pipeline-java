package com.github.lechandonga.dataqualitypipelinejava.config;

import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 容量与并发边界配置（前缀 dqp.*）。
 *
 * @param workerThreads          单批次并行评估的工作线程数
 * @param maxConcurrentBatches   同时在执行的批次数上限
 * @param queueCapacity          单批次在途记录队列上限（背压/隔离依据）
 * @param distinctCapacity       唯一键/基数集合的条目上限（防止超大输入无界占用）
 * @param overflowPolicy         达到上限时 ISOLATE（隔离记录）或 REJECT（整批拒绝）
 * @param meanShiftThreshold     均值漂移阈值（以基线标准差为单位，z-score）
 * @param stdShiftThreshold      标准差漂移阈值（倍数）
 * @param baselineDir            基线持久化目录；null/空表示仅内存
 */
@ConfigurationProperties(prefix = "dqp")
public record PipelineProperties(
        int workerThreads,
        int maxConcurrentBatches,
        int queueCapacity,
        int distinctCapacity,
        OverflowPolicy overflowPolicy,
        double meanShiftThreshold,
        double stdShiftThreshold,
        String baselineDir
) {

    public PipelineProperties {
        if (workerThreads <= 0) {
            workerThreads = 4;
        }
        if (maxConcurrentBatches <= 0) {
            maxConcurrentBatches = 8;
        }
        if (queueCapacity <= 0) {
            queueCapacity = 10_000;
        }
        if (distinctCapacity <= 0) {
            distinctCapacity = 1_000_000;
        }
        if (overflowPolicy == null) {
            overflowPolicy = OverflowPolicy.ISOLATE;
        }
        if (!(meanShiftThreshold > 0.0)) {
            meanShiftThreshold = 3.0;
        }
        if (!(stdShiftThreshold > 0.0)) {
            stdShiftThreshold = 2.0;
        }
        if (baselineDir != null && baselineDir.isBlank()) {
            baselineDir = null;
        }
    }

    /** 默认配置，供测试与无配置启动使用。 */
    public static PipelineProperties defaults() {
        return new PipelineProperties(4, 8, 10_000, 1_000_000,
                OverflowPolicy.ISOLATE, 3.0, 2.0, null);
    }
}
