package com.github.lechandonga.dataqualitypipelinejava.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.dataqualitypipelinejava.api.dto.RecordLine;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveCorruptedException;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.BatchReplayRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineCapacityException;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayReport;
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
import org.springframework.http.HttpStatus;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.List;
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
    public ReplayReport replay(@PathVariable String batchId, BufferedReader reader) {
        log.info("batch replay requested: batchId={}", batchId);
        return pipeline.replayWithReport(batchId, new NdjsonIterator(reader, objectMapper));
    }

    /**
     * 导出当前仍被隔离的记录为 NDJSON 修复文件：记录标识、原始顺序、原始字段一字不改，
     * 修复后可直接回灌 {@code /replay}。
     */
    @GetMapping(value = "/{batchId}/quarantine/export",
            produces = "application/x-ndjson;charset=UTF-8")
    public ResponseEntity<String> exportQuarantine(@PathVariable String batchId) {
        List<DataRecord> records = pipeline.exportQuarantined(batchId);
        StringBuilder sb = new StringBuilder();
        for (DataRecord r : records) {
            sb.append(toNdjson(r)).append('\n');
        }
        return ResponseEntity.ok()
                .header("Content-Disposition",
                        "attachment; filename=\"" + batchId + "-quarantine.ndjson\"")
                .body(sb.toString());
    }

    @GetMapping("/{batchId}")
    public ResponseEntity<BatchResult> result(@PathVariable String batchId) {
        return switch (pipeline.lookup(batchId)) {
            case PipelineService.Lookup.Found found -> ResponseEntity.ok(found.result());
            // 410：曾经存在但已被逐出且无存档，显式区别于 404"从未存在"
            case PipelineService.Lookup.Evicted e -> ResponseEntity.status(HttpStatus.GONE)
                    .header("X-Batch-Status", "EVICTED_NOT_AVAILABLE")
                    .build();
            case PipelineService.Lookup.NotFound ignored -> ResponseEntity.notFound().build();
            // 500：磁盘存档损坏，绝不静默当成不存在
            case PipelineService.Lookup.Corrupt c -> throw new BatchArchiveCorruptedException(
                    "batch archive corrupted on disk: " + c.message());
        };
    }

    private String toNdjson(DataRecord record) {
        try {
            return objectMapper.writeValueAsString(new RecordLine(record.id(), record.data()));
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize record " + record.id(), e);
        }
    }

    @ExceptionHandler(PipelineCapacityException.class)
    public ResponseEntity<Map<String, String>> capacity(PipelineCapacityException ex) {
        log.warn("capacity rejection: batchId={}, message={}", ex.batchId(), ex.getMessage());
        return ResponseEntity.status(429).body(Map.of(
                "batchId", ex.batchId(),
                "reason", "CAPACITY_EXCEEDED",
                "message", String.valueOf(ex.getMessage())));
    }

    /** 重放输入含非隔离清单记录：整体拒绝（422，原因码可区分），批次结论不变。 */
    @ExceptionHandler(BatchReplayRejectedException.class)
    public ResponseEntity<Map<String, Object>> replayRejected(BatchReplayRejectedException ex) {
        log.warn("replay rejected as a whole: batchId={}, records={}",
                ex.batchId(), ex.rejectedRecordIds());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                "batchId", ex.batchId(),
                "reason", ex.reasonCode(),
                "rejectedRecordIds", ex.rejectedRecordIds(),
                "message", ex.getMessage()));
    }

    /** 批次曾经存在但已逐出内存、又没有存档可恢复：410。 */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> illegalState(IllegalStateException ex) {
        String message = String.valueOf(ex.getMessage());
        boolean evicted = message.contains("evicted");
        return ResponseEntity.status(evicted ? HttpStatus.GONE : HttpStatus.CONFLICT)
                .body(Map.of("reason", evicted ? "EVICTED_NOT_AVAILABLE" : "REPLAY_CONFLICT",
                        "message", message));
    }

    /** 批次不存在 / 非法输入：404 或 400。 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> illegalArgument(IllegalArgumentException ex) {
        String message = String.valueOf(ex.getMessage());
        boolean missing = message.startsWith("batch not found");
        return ResponseEntity.status(missing ? HttpStatus.NOT_FOUND : HttpStatus.BAD_REQUEST)
                .body(Map.of("reason", missing ? "BATCH_NOT_FOUND" : "BAD_REQUEST",
                        "message", message));
    }

    /** 存档损坏：500，显式失败。 */
    @ExceptionHandler(BatchArchiveCorruptedException.class)
    public ResponseEntity<Map<String, String>> corrupt(BatchArchiveCorruptedException ex) {
        log.error("batch archive corruption detected: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "reason", "ARCHIVE_CORRUPTED",
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
