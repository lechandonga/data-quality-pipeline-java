package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;

/**
 * 不可变规则集快照，批次执行期间持有该对象即可保证不受更新影响。
 */
public record RuleSet(
        String version,
        List<RuleDefinition> rules) {
}
