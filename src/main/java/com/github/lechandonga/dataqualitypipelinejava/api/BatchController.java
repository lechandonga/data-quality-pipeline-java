package com.github.lechandonga.dataqualitypipelinejava.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.dataqualitypipelinejava.api.dto.RecordLine;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineCapacityException;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/** 批次的流式执行、结果查询与隔离记录重放。请求体为 NDJSON（每行一条记录）。 */
@RestController
@RequestMapping("/api/batches")
public class BatchController {

    private static final Logger log = LoggerFactory.getLogger(BatchController.class);

    private final PipelineService pipeline;
    private final ObjectMapper objectMapper;

    public BatchController(PipelineService pipeline, ObjectMapper objectMapper) {
        this.pipeline = pipeline;
        this.objectMapper = objectMapper;
    }

    @PostMapping(value = "/{batchId}/execute", consumes = "application/x-ndjson")
    public BatchResult execute(@PathVariable String batchId,
                               @RequestParam String dataset,
                               @RequestParam(defaultValue = "false") boolean commit,
                               @RequestParam(required = false) Integer workers,
                               BufferedReader reader) {
        log.info("batch execution requested: batchId={}, dataset={}, commit={}", batchId, dataset, commit);
        return pipeline.execute(
                new ExecutionRequest(batchId, dataset, commit, workers, null),
                new NdjsonIterator(reader, objectMapper));
    }

    @PostMapping(value = "/{batchId}/replay", consumes = "application/x-ndjson")
    public BatchResult replay(@PathVariable String batchId, BufferedReader reader) {
        log.info("batch replay requested: batchId={}", batchId);
        return pipeline.replay(batchId, new NdjsonIterator(reader, objectMapper));
    }

    @GetMapping("/{batchId}")
    public ResponseEntity<BatchResult> result(@PathVariable String batchId) {
        return pipeline.resultOf(batchId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler(PipelineCapacityException.class)
    public ResponseEntity<Map<String, String>> capacity(PipelineCapacityException ex) {
        log.warn("capacity rejection: batchId={}, message={}", ex.batchId(), ex.getMessage());
        return ResponseEntity.status(429).body(Map.of(
                "batchId", ex.batchId(),
                "reason", "CAPACITY_EXCEEDED",
                "message", String.valueOf(ex.getMessage())));
    }

    /** 逐行懒解析 NDJSON 的迭代器，不把请求体全量读入内存。 */
    static final class NdjsonIterator implements Iterator<DataRecord> {

        private final BufferedReader reader;
        private final ObjectMapper mapper;
        private long order;
        private String pendingLine;

        NdjsonIterator(BufferedReader reader, ObjectMapper mapper) {
            this.reader = reader;
            this.mapper = mapper;
            advance();
        }

        private void advance() {
            try {
                String line;
                do {
                    line = reader.readLine();
                } while (line != null && line.isBlank());
                pendingLine = line;
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to read NDJSON stream", e);
            }
        }

        @Override
        public boolean hasNext() {
            return pendingLine != null;
        }

        @Override
        public DataRecord next() {
            if (pendingLine == null) {
                throw new NoSuchElementException();
            }
            try {
                RecordLine line = mapper.readValue(pendingLine, RecordLine.class);
                String id = line.id() != null ? line.id() : ("row-" + order);
                DataRecord record = new DataRecord(id, order++,
                        line.data() == null ? Map.of() : line.data());
                advance();
                return record;
            } catch (Exception e) {
                throw new IllegalArgumentException("malformed NDJSON line: " + pendingLine, e);
            }
        }
    }
}
