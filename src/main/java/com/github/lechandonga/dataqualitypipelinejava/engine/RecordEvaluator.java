package com.github.lechandonga.dataqualitypipelinejava.engine;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRule;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRuleSet;
import com.github.lechandonga.dataqualitypipelinejava.rule.UniqueTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 单条记录求值器：应用规则集快照中的全部规则，
 * 并通过 {@link UniqueTracker} 完成批次内 UNIQUE 判定。
 *
 * <p>纯顺序求值：规则按发布顺序执行，收集全部失败后返回，
 * 同输入必然同输出，与并发度无关。</p>
 */
final class RecordEvaluator {

    private final CompiledRuleSet ruleSet;
    private final UniqueTracker uniqueTracker;

    RecordEvaluator(CompiledRuleSet ruleSet, UniqueTracker uniqueTracker) {
        this.ruleSet = ruleSet;
        this.uniqueTracker = uniqueTracker;
    }

    List<ValidationFailure> evaluate(Map<String, String> record) {
        List<ValidationFailure> failures = new ArrayList<>();
        for (CompiledRule rule : ruleSet.orderedRules()) {
            if (rule.type() == RuleType.UNIQUE) {
                String value = record.get(rule.field());
                boolean blank = value == null || value.trim().isEmpty();
                if (rule.definition().nullable() && blank) {
                    continue;
                }
                String observed = blank ? "" : value;
                if (!uniqueTracker.observe(rule.definition(), observed)) {
                    failures.add(new ValidationFailure(
                            rule.id(), RuleType.UNIQUE.name(), rule.field(),
                            "字段取值在批次内不唯一: " + observed));
                }
                continue;
            }
            failures.addAll(rule.evaluate(record));
        }
        return failures;
    }
}
