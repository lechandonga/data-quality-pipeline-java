package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleSet;

import java.util.List;

/**
 * 已编译并通过全部结构校验的规则集快照（不可变）。
 *
 * <p>批次执行期间持有该对象：规则更新只替换注册表中的引用，
 * 进行中的批次持有的快照不变，因此更新要么对该批次完全不可见，
 * 要么在下一批次整体生效，不存在半更新状态。</p>
 */
public record CompiledRuleSet(
        RuleSet ruleSet,
        List<CompiledRule> orderedRules) {

    public String version() {
        return ruleSet.version();
    }

    public List<RuleDefinition> definitions() {
        return ruleSet.rules();
    }
}
