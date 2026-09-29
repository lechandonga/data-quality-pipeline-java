package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "dqp.worker-threads=2",
        "dqp.queue-capacity=64",
        "dqp.distinct-capacity=1000000"
})
@AutoConfigureMockMvc
class PipelineIntegrationTest {

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    private static final MediaType NDJSON =
            MediaType.parseMediaType("application/x-ndjson");

    @Test
    void invalidRuleUpdateIsRejectedWithDistinguishableReason() throws Exception {
        String body = """
                {"rules":[
                  {"id":"bad","type":"RANGE","field":"age","min":"100","max":"0"}
                ]}
                """;
        mvc.perform(put("/api/rules").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("INVALID_RANGE"))
                .andExpect(jsonPath("$.ruleId").value("bad"));
    }

    @Test
    void endToEndExecuteQueryReplayAndCommit() throws Exception {
        String rules = """
                {"rules":[
                  {"id":"nn-name","type":"NOT_NULL","field":"name"},
                  {"id":"rx-age","type":"REGEX","field":"age","pattern":"\\\\d+"},
                  {"id":"u-id","type":"UNIQUE","children":["id"]}
                ]}
                """;
        mvc.perform(put("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").isNotEmpty());

        String ndjson = """
                {"id":"r1","data":{"id":"k1","name":"a","age":"30"}}
                {"id":"r2","data":{"id":"k2","name":"","age":"30"}}
                {"id":"r3","data":{"id":"k1","name":"c","age":"30"}}
                """;
        mvc.perform(post("/api/batches/int-1/execute")
                        .param("dataset", "people").param("commit", "true")
                        .contentType(NDJSON).content(ndjson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMMITTED"))
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.passedCount").value(1))
                .andExpect(jsonPath("$.quarantinedCount").value(2))
                .andExpect(jsonPath("$.baselineUpdated").value(true));

        mvc.perform(get("/api/batches/int-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.batchId").value("int-1"));

        // 修复 r2（补名字）；r3 仍与 r1 唯一冲突
        String repaired = """
                {"id":"r2","data":{"id":"k2","name":"fixed","age":"30"}}
                """;
        mvc.perform(post("/api/batches/int-1/replay").contentType(NDJSON).content(repaired))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.batch.total").value(3))
                .andExpect(jsonPath("$.batch.passedCount").value(2))
                .andExpect(jsonPath("$.batch.quarantinedCount").value(1))
                .andExpect(jsonPath("$.nowPassed[0]").value("r2"))
                .andExpect(jsonPath("$.stillQuarantined[0]").value("r3"));
    }

    @Test
    void exportQuarantineAndForeignRecordWholeReplayRejection() throws Exception {
        String rules = """
                {"rules":[
                  {"id":"nn-name","type":"NOT_NULL","field":"name"},
                  {"id":"u-id","type":"UNIQUE","children":["id"]}
                ]}
                """;
        mvc.perform(put("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isOk());

        String ndjson = """
                {"id":"r1","data":{"id":"k1","name":"a"}}
                {"id":"r2","data":{"id":"k2","name":""}}
                """;
        mvc.perform(post("/api/batches/int-export/execute")
                        .param("dataset", "people")
                        .contentType(NDJSON).content(ndjson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.persistenceStatus").value("IN_MEMORY_ONLY"));

        // 导出：原样 NDJSON，只含仍隔离的 r2，保留原始字段
        String exported = mvc.perform(get("/api/batches/int-export/quarantine/export"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertTrue(exported.contains("\"id\":\"r2\""));
        assertFalse(exported.contains("\"id\":\"r1\""));

        // 原样回灌重放：r2 仍因空名字隔离，整批结论不变
        mvc.perform(post("/api/batches/int-export/replay")
                        .contentType(NDJSON).content(exported))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.batch.quarantinedCount").value(1))
                .andExpect(jsonPath("$.stillQuarantined[0]").value("r2"));

        // 混入已通过的 r1：整体拒绝 422，不可部分重放
        String mixed = """
                {"id":"r2","data":{"id":"k2","name":"fixed"}}
                {"id":"r1","data":{"id":"k1","name":"a"}}
                """;
        mvc.perform(post("/api/batches/int-export/replay")
                        .contentType(NDJSON).content(mixed))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("NOT_IN_QUARANTINE"));

        // 结论未被部分重放改动：r2 仍隔离
        mvc.perform(get("/api/batches/int-export"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.quarantinedCount").value(1));

        // 不存在的批次：404；重放同样区分原因
        mvc.perform(get("/api/batches/no-such-batch")).andExpect(status().isNotFound());
    }

    @Test
    void firstRunOverApiShowsNoBaselineWithoutFalseDrift() throws Exception {
        String rules = """
                {"rules":[{"id":"nn-v","type":"NOT_NULL","field":"v"}]}
                """;
        mvc.perform(put("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isOk());
        String ndjson = """
                {"id":"a","data":{"v":"1"}}
                {"id":"b","data":{"v":"2"}}
                """;
        mvc.perform(post("/api/batches/int-first/execute")
                        .param("dataset", "brand-new-dataset")
                        .contentType(NDJSON).content(ndjson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.driftFindings[0].status").value("NO_BASELINE"))
                .andExpect(jsonPath("$.baselineUpdated").value(false));
    }

    @Test
    void largeStreamingInputIsFullyProcessedWithBoundedMemory() throws Exception {
        String rules = """
                {"rules":[
                  {"id":"r-v","type":"RANGE","field":"v","min":"0","max":"10000000"},
                  {"id":"u-id","type":"UNIQUE","children":["id"]}
                ]}
                """;
        mvc.perform(put("/api/rules").contentType(MediaType.APPLICATION_JSON).content(rules))
                .andExpect(status().isOk());

        List<String> lines = new ArrayList<>();
        int n = 100_000;
        for (int i = 0; i < n; i++) {
            lines.add("{\"id\":\"" + i + "\",\"data\":{\"id\":\"" + i + "\",\"v\":\"" + i + "\"}}");
        }
        String body = String.join("\n", lines);
        mvc.perform(post("/api/batches/int-big/execute")
                        .param("dataset", "big-ds").param("workers", "4")
                        .contentType(NDJSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(n))
                .andExpect(jsonPath("$.passedCount").value(n))
                .andExpect(jsonPath("$.quarantinedCount").value(0))
                .andExpect(jsonPath("$.overflowIsolatedCount").value(0));
    }
}
