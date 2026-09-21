package com.github.lechandonga.dataqualitypipelinejava.rule;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 编译后的不可变规则快照。批次开始时绑定一个快照，运行期间的规则更新
 * 只会产生新快照，绝不影响进行中的批次。
 */
public final class RuleSet {

    private final String version;
    private final List<RowRule> rowRules;
    private final List<UniqueRule> uniqueRules;
    private final Map<String, RowRule> rowRuleById;

    public RuleSet(String version, List<RowRule> rowRules, List<UniqueRule> uniqueRules) {
        this.version = version;
        this.rowRules = List.copyOf(rowRules);
        this.uniqueRules = List.copyOf(uniqueRules);
        Map<String, RowRule> index = new LinkedHashMap<>();
        for (RowRule r : rowRules) {
            index.put(r.id(), r);
        }
        this.rowRuleById = Map.copyOf(index);
    }

    public String version() {
        return version;
    }

    public List<RowRule> rowRules() {
        return rowRules;
    }

    public List<UniqueRule> uniqueRules() {
        return uniqueRules;
    }

    public RowRule rowRule(String id) {
        return rowRuleById.get(id);
    }
}
