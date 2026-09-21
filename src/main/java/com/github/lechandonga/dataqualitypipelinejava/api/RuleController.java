package com.github.lechandonga.dataqualitypipelinejava.api;

import com.github.lechandonga.dataqualitypipelinejava.api.dto.RuleSetUpdateRequest;
import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** 规则集查询与整体更新（更新要么整体生效要么完全不生效）。 */
@RestController
@RequestMapping("/api/rules")
public class RuleController {

    private static final Logger log = LoggerFactory.getLogger(RuleController.class);

    private final RuleRegistry registry;

    public RuleController(RuleRegistry registry) {
        this.registry = registry;
    }

    @GetMapping
    public Map<String, Object> current() {
        RuleSet set = registry.current();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", set.version());
        body.put("rowRuleIds", set.rowRules().stream().map(r -> r.id()).toList());
        body.put("uniqueRuleIds", set.uniqueRules().stream().map(u -> u.id()).toList());
        return body;
    }

    @PutMapping
    public Map<String, Object> replace(@RequestBody RuleSetUpdateRequest request) {
        // 编译失败会抛 InvalidRuleException，旧规则集完全保持不变
        RuleSet set = registry.replaceAll(request.rules() == null
                ? java.util.List.of() : request.rules());
        log.info("rule set updated via API: version={}", set.version());
        return Map.of("version", set.version(),
                "rowRules", set.rowRules().size(),
                "uniqueRules", set.uniqueRules().size());
    }

    @ExceptionHandler(InvalidRuleException.class)
    public ResponseEntity<Map<String, String>> invalid(InvalidRuleException ex) {
        log.warn("rule update rejected: ruleId={}, reason={}, message={}",
                ex.ruleId(), ex.reason(), ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "ruleId", String.valueOf(ex.ruleId()),
                "reason", ex.reason().name(),
                "message", String.valueOf(ex.getMessage())));
    }
}
