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
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.passedCount").value(2))
                .andExpect(jsonPath("$.quarantinedCount").value(1));
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
