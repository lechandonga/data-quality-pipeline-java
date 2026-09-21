package com.github.lechandonga.dataqualitypipelinejava.api.dto;

import java.util.Map;

/** NDJSON 每行结构：{"id":"...","data":{"f":"v"}}。 */
public record RecordLine(String id, Map<String, String> data) {
}
