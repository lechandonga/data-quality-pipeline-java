package com.github.lechandonga.dataqualitypipelinejava.web;

import com.github.lechandonga.dataqualitypipelinejava.engine.BatchRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
import com.github.lechandonga.dataqualitypipelinejava.engine.QualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.QuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRuleSet;
import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 本地验证用 REST 入口：
 *
 * <ul>
 *   <li>POST /api/rules                       整体发布规则集（非法则整体 422 拒绝）</li>
 *   <li>GET  /api/rules/current               查看当前规则版本</li>
 *   <li>POST /api/batches                     执行批次（逐条投喂，不全量驻留）</li>
 *   <li>GET  /api/batches/{id}/quarantine     查看隔离记录与规则/字段级原因</li>
 *   <li>POST /api/batches/{id}/replay         重放修复后的隔离记录</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class PipelineController {

    private final RuleSetManager ruleSetManager;
    private final QualityPipeline pipeline;
    private final QuarantineService quarantineService;

    public PipelineController(RuleSetManager ruleSetManager,
                              QualityPipeline pipeline,
                              QuarantineService quarantineService) {
        this.ruleSetManager = ruleSetManager;
        this.pipeline = pipeline;
        this.quarantineService = quarantineService;
    }

    @PostMapping("/rules")
    public PublishRulesApiResponse publishRules(@RequestBody ApiDtos.PublishRulesRequest request) {
        if (request == null || request.rules() == null || request.rules().isEmpty()) {
            throw new InvalidRuleException("RULESET_EMPTY", "发布的规则集不能为空");
        }
        CompiledRuleSet snapshot = ruleSetManager.publish(request.rules());
        return new PublishRulesApiResponse(snapshot.version(), snapshot.orderedRules().size());
    }

    @GetMapping("/rules/current")
    public PublishRulesApiResponse currentRules() {
        CompiledRuleSet current = ruleSetManager.current();
        if (current == null) {
            throw new BatchRejectedException("RULESET_MISSING", "尚未发布任何规则集");
        }
        return new PublishRulesApiResponse(current.version(), current.orderedRules().size());
    }

    @PostMapping("/batches")
    public BatchResult executeBatch(@RequestBody ApiDtos.ExecuteBatchRequest request) {
        if (request == null || request.records() == null) {
            throw new BatchRejectedException("REQUEST_INVALID", "批次请求必须包含 records");
        }
        Stream<Map<String, String>> stream = request.records().stream();
        return pipeline.execute(request.batchId(), stream);
    }

    @GetMapping("/batches/{batchId}/quarantine")
    public List<RecordResult> quarantine(@PathVariable String batchId) {
        return quarantineService.list(batchId);
    }

    @PostMapping("/batches/{batchId}/replay")
    public BatchResult replay(@PathVariable String batchId,
                              @RequestBody ApiDtos.ReplayRequest request) {
        if (request == null || request.records() == null) {
            throw new BatchRejectedException("REQUEST_INVALID", "重放请求必须包含 records");
        }
        List<QuarantineService.ReplayInput> inputs = request.records().stream()
                .map(m -> new QuarantineService.ReplayInput(m.get("id"), m))
                .toList();
        return pipeline.replay(batchId, inputs);
    }

    public record PublishRulesApiResponse(String version, int ruleCount) {
    }

    @ExceptionHandler(InvalidRuleException.class)
    public ResponseEntity<ApiDtos.ErrorResponse> handleInvalidRule(InvalidRuleException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new ApiDtos.ErrorResponse(e.reasonCode(), e.getMessage()));
    }

    @ExceptionHandler(BatchRejectedException.class)
    public ResponseEntity<ApiDtos.ErrorResponse> handleRejected(BatchRejectedException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ApiDtos.ErrorResponse(e.reasonCode(), e.getMessage()));
    }

    @ExceptionHandler(PipelineException.class)
    public ResponseEntity<ApiDtos.ErrorResponse> handlePipeline(PipelineException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiDtos.ErrorResponse(e.reasonCode(), e.getMessage()));
    }
}
