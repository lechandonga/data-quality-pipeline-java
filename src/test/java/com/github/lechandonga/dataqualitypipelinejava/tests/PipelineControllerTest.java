package com.github.lechandonga.dataqualitypipelinejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 边界：发布、执行、隔离、重放与非法规则的区分化错误码。
 */
@SpringBootTest
@AutoConfigureMockMvc
class PipelineControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void endToEndPublishExecuteQuarantineReplay() throws Exception {
        String rules = """
                { "rules": [
                  { "id": "email-nn", "type": "NOT_NULL", "field": "email" }
                ]}
                """;
        mvc.perform(post("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value("v1"))
                .andExpect(jsonPath("$.ruleCount").value(1));

        String batch = """
                { "batchId": "b-rest", "records": [
                  { "id": "1", "email": "a@x" },
                  { "id": "2" }
                ]}
                """;
        mvc.perform(post("/api/batches").contentType(MediaType.APPLICATION_JSON).content(batch))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMMITTED"))
                .andExpect(jsonPath("$.passedCount").value(1))
                .andExpect(jsonPath("$.failedCount").value(1));

        mvc.perform(get("/api/batches/b-rest/quarantine"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].recordKey").value("2"))
                .andExpect(jsonPath("$[0].failures[0].ruleId").value("email-nn"))
                .andExpect(jsonPath("$[0].failures[0].field").value("email"));

        String replay = """
                { "records": [ { "id": "2", "email": "b@x" } ] }
                """;
        mvc.perform(post("/api/batches/b-rest/replay")
                        .contentType(MediaType.APPLICATION_JSON).content(replay))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.passedCount").value(1))
                .andExpect(jsonPath("$.cumulativePassed").value(2))
                .andExpect(jsonPath("$.status").value("COMMITTED"));

        mvc.perform(get("/api/batches/b-rest/quarantine"))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void invalidRuleReturnsDistinctReasonCode() throws Exception {
        String rules = """
                { "rules": [
                  { "id": "bad", "type": "RANGE", "field": "age",
                    "params": { "min": "10", "max": "1" } }
                ]}
                """;
        mvc.perform(post("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reasonCode").value("RANGE_INVALID"));
    }

    @Test
    void replayUnknownBatchReturns409WithReasonCode() throws Exception {
        String replay = """
                { "records": [] }
                """;
        mvc.perform(post("/api/batches/no-such-batch-xyz/replay")
                        .contentType(MediaType.APPLICATION_JSON).content(replay))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reasonCode").value("BATCH_NOT_FOUND"));
    }
}
