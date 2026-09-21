package com.github.lechandonga.dataqualitypipelinejava.api.dto;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;

import java.util.List;

/** 规则集整体替换请求体。 */
public record RuleSetUpdateRequest(List<RuleDefinition> rules) {
}
