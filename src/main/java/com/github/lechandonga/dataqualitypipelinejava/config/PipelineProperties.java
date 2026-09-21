package com.github.lechandonga.dataqualitypipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 容量与并发边界配置。
 *
 * <ul>
 *   <li>maxRecordsPerBatch：单批次最大记录数（硬上限），超限按 overflowStrategy 处理</li>
 *   <li>maxConcurrentBatches：同时在执行的批次数（信号量），超出需排队，不占额外资源</li>
 *   <li>recordQueueCapacity：生产者/消费者之间有界队列容量，满时背压</li>
 *   <li>offerTimeoutMillis：队列满时入队最长等待毫秒，超时按容量策略处理</li>
 *   <li>uniqueTrackerCapacity：批次内唯一性不同值总槽位上限</li>
 *   <li>overflowStrategy：REJECT 或 ISOLATE</li>
 *   <li>maxRejectRatio：失败记录占比超过该阈值时批次被拒绝（默认 1.0 表示永不）</li>
 *   <li>keyField：记录主键字段；缺省时用记录序号（仅用于定位隔离记录）</li>
 *   <li>baselineFields：参与基线统计/漂移检查的字段（逗号分隔，空表示不启用）</li>
 *   <li>driftZThreshold / nullRatioDelta：漂移检查阈值</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    private int maxRecordsPerBatch = 1_000_000;
    private int maxConcurrentBatches = 4;
    private int recordQueueCapacity = 1024;
    private long offerTimeoutMillis = 5_000L;
    private int uniqueTrackerCapacity = 1_000_000;
    private String overflowStrategy = "REJECT";
    private double maxRejectRatio = 1.0;
    private String keyField = "id";
    private String baselineFields = "";
    private double driftZThreshold = 3.0;
    private double nullRatioDelta = 0.1;
    private String baselineStore = "file";
    private String storagePath = "data/pipeline";

    public int getMaxRecordsPerBatch() { return maxRecordsPerBatch; }
    public void setMaxRecordsPerBatch(int v) { this.maxRecordsPerBatch = v; }
    public int getMaxConcurrentBatches() { return maxConcurrentBatches; }
    public void setMaxConcurrentBatches(int v) { this.maxConcurrentBatches = v; }
    public int getRecordQueueCapacity() { return recordQueueCapacity; }
    public void setRecordQueueCapacity(int v) { this.recordQueueCapacity = v; }
    public long getOfferTimeoutMillis() { return offerTimeoutMillis; }
    public void setOfferTimeoutMillis(long v) { this.offerTimeoutMillis = v; }
    public int getUniqueTrackerCapacity() { return uniqueTrackerCapacity; }
    public void setUniqueTrackerCapacity(int v) { this.uniqueTrackerCapacity = v; }
    public String getOverflowStrategy() { return overflowStrategy; }
    public void setOverflowStrategy(String v) { this.overflowStrategy = v; }
    public double getMaxRejectRatio() { return maxRejectRatio; }
    public void setMaxRejectRatio(double v) { this.maxRejectRatio = v; }
    public String getKeyField() { return keyField; }
    public void setKeyField(String v) { this.keyField = v; }
    public String getBaselineFields() { return baselineFields; }
    public void setBaselineFields(String v) { this.baselineFields = v; }
    public double getDriftZThreshold() { return driftZThreshold; }
    public void setDriftZThreshold(double v) { this.driftZThreshold = v; }
    public double getNullRatioDelta() { return nullRatioDelta; }
    public void setNullRatioDelta(double v) { this.nullRatioDelta = v; }
    public String getBaselineStore() { return baselineStore; }
    public void setBaselineStore(String v) { this.baselineStore = v; }
    public String getStoragePath() { return storagePath; }
    public void setStoragePath(String v) { this.storagePath = v; }
}
