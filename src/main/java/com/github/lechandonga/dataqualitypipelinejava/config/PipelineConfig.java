package com.github.lechandonga.dataqualitypipelinejava.config;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.FileBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchive;
import com.github.lechandonga.dataqualitypipelinejava.archive.FileBatchArchive;
import com.github.lechandonga.dataqualitypipelinejava.archive.InMemoryBatchArchive;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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

    @Bean
    public BatchArchive batchArchive(PipelineProperties props) {
        if (props.archiveDir() != null && !props.archiveDir().isBlank()) {
            return new FileBatchArchive(props.archiveDir());
        }
        return new InMemoryBatchArchive();
    }
}
