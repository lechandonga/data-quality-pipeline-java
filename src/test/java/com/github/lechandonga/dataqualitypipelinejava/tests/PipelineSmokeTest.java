package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PipelineSmokeTest {

    private DefaultQualityPipeline newPipeline(PipelineProperties props) {
        return new DefaultQualityPipeline(props, new RuleSetManager(),
                new InMemoryBaselineStore(), new InMemoryQuarantineService());
    }

    private RuleSetManager rules(RuleDefinition... defs) {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(defs));
        return manager;
    }

    private RuleDefinition notNull(String id, String field) {
        return new RuleDefinition(id, RuleType.NOT_NULL, field, Map.of(), false);
    }

    @Test
    void allPassAndCommit() {
        RuleSetManager manager = rules(notNull("r1", "name"));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(
                new PipelineProperties(), manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        BatchResult result = pipeline.execute("b1",
                Stream.of(Map.of("name", "a"), Map.of("name", "b")));

        assertEquals(BatchStatus.COMMITTED, result.status());
        assertEquals(2, result.totalCount());
        assertEquals(2, result.passedCount());
        assertEquals(0, result.failedCount());
    }

    @Test
    void invalidRuleRejectedWithReasonCode() {
        RuleDefinition bad = new RuleDefinition("bad", RuleType.RANGE, "age",
                Map.of("min", "10", "max", "1"), false);
        RuleSetManager manager = new RuleSetManager();
        PipelineException ex = assertThrows(PipelineException.class,
                () -> manager.publish(List.of(bad)));
        assertEquals("RANGE_INVALID", ex.reasonCode());
    }
}
