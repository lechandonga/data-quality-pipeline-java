package com.github.lechandonga.dataqualitypipelinejava.config;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.FileBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.engine.QualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.QuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 流水线 Spring 装配。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfiguration {

    @Bean
    public RuleSetManager ruleSetManager() {
        return new RuleSetManager();
    }

    /**
     * pipeline.baseline-store=file（默认）时使用本地文件存储，重启后基线持续累积；
     * 设为 memory 时使用纯内存存储（测试/演示）。
     */
    @Bean
    public BaselineStore baselineStore(PipelineProperties properties) {
        if ("memory".equalsIgnoreCase(properties.getBaselineStore())) {
            return new InMemoryBaselineStore();
        }
        return new FileBaselineStore(Path.of(properties.getStoragePath(), "baselines"));
    }

    @Bean
    public QuarantineService quarantineService() {
        return new InMemoryQuarantineService();
    }

    @Bean
    public QualityPipeline qualityPipeline(PipelineProperties properties,
                                           RuleSetManager ruleSetManager,
                                           BaselineStore baselineStore,
                                           QuarantineService quarantineService) {
        return new DefaultQualityPipeline(properties, ruleSetManager, baselineStore, quarantineService);
    }
}
