package com.github.lechandonga.dataqualitypipelinejava.config;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.FileBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveStore;
import com.github.lechandonga.dataqualitypipelinejava.archive.FileBatchArchiveStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 流水线装配：默认参数、属性绑定，以及基线存储选择
 * （配置 dqp.baseline-dir 时用文件原子存储，否则用内存存储）。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    @Bean
    public BaselineStore baselineStore(PipelineProperties props) {
        if (props.baselineDir() != null && !props.baselineDir().isBlank()) {
            return new FileBaselineStore(props.baselineDir());
        }
        return new InMemoryBaselineStore();
    }

    /**
     * 批次结论存档：配置 dqp.archive-dir 后启用"临时文件 + 原子 move"的本地存档；
     * 未配置时为 null，批次结论仅存内存（行为明确，不是静默丢失）。
     */
    @Bean
    @ConditionalOnProperty(name = "dqp.archive-dir")
    public BatchArchiveStore batchArchiveStore(PipelineProperties props) {
        return new FileBatchArchiveStore(props.archiveDir());
    }
}
