package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 当前生效规则集的注册表。
 * 更新采用 copy-on-write + 先编译后替换：要么整体生效，要么完全不生效，
 * 不存在半更新状态；进行中的批次持有旧快照引用不受影响。
 */
@Component
public class RuleRegistry {

    private static final Logger log = LoggerFactory.getLogger(RuleRegistry.class);

    private final RuleCompiler compiler;
    private final AtomicReference<RuleSet> current = new AtomicReference<>();

    public RuleRegistry(RuleCompiler compiler) {
        this.compiler = compiler;
    }

    /**
     * 原子替换规则集。先完整编译，编译抛 {@link InvalidRuleException} 时
     * 旧规则集保持不变（完全不生效）。
     */
    public RuleSet replaceAll(List<RuleDefinition> definitions) {
        RuleSet compiled = compiler.compile(definitions);
        return publish(compiled);
    }

    /**
     * 直接发布一个已编译快照（主要用于测试注入自定义规则实现）。
     * 同样是整体原子替换。
     */
    public RuleSet publish(RuleSet set) {
        current.set(set);
        log.info("rules replaced atomically: version={}, rowRules={}, uniqueRules={}",
                set.version(), set.rowRules().size(), set.uniqueRules().size());
        return set;
    }

    /** 当前生效快照；从未发布时返回一个空规则集（确定性的"无约束"状态）。 */
    public RuleSet current() {
        RuleSet set = current.get();
        if (set != null) {
            return set;
        }
        return compiler.compile(List.of());
    }
}
