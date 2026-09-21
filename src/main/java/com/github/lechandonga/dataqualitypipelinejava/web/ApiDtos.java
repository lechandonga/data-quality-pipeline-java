package com.github.lechandonga.dataqualitypipelinejava.web;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;

import java.util.List;
import java.util.Map;

/**
 * 本地 REST 接口的数据传输对象。
 */
public final class ApiDtos {

    private ApiDtos() {
    }

    public record PublishRulesRequest(List<RuleDefinition> rules) {
    }

    public record ExecuteBatchRequest(String batchId, List<Map<String, String>> records) {
    }

    public record ReplayRequest(List<Map<String, String>> records) {
    }

    public record ErrorResponse(String reasonCode, String message) {
    }
}
