package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleSet;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 规则集注册表。
 *
 * <p>发布采用“先全部校验编译、成功后整体替换引用”的策略：
 * 任一规则非法、子规则引用不存在或组合成环，整个发布被拒绝，已发布版本保持不变；
 * 发布成功后新批次看到新版本，进行中的批次仍持有旧快照。</p>
 */
public class RuleSetManager {

    private final AtomicReference<CompiledRuleSet> current = new AtomicReference<>();
    private final AtomicLong versionSequence = new AtomicLong();

    public RuleSetManager() {
    }

    /**
     * @return 当前已发布快照；从未发布时返回 null
     */
    public CompiledRuleSet current() {
        return current.get();
    }

    /**
     * 校验并整体发布新规则集。
     *
     * @throws InvalidRuleException 任一规则非法时整体拒绝（原因码可区分）
     */
    public synchronized CompiledRuleSet publish(List<RuleDefinition> definitions) {
        List<RuleDefinition> copy = List.copyOf(definitions);
        Map<String, RuleDefinition> byId = new LinkedHashMap<>();
        for (RuleDefinition definition : copy) {
            RuleValidator.validate(definition);
            String id = definition.id().trim();
            if (byId.putIfAbsent(id, definition) != null) {
                throw new InvalidRuleException("RULE_ID_DUPLICATE",
                        "规则 id 重复: " + id);
            }
        }
        Map<String, CompiledRule> compiled = new LinkedHashMap<>();
        for (RuleDefinition definition : copy) {
            if (definition.type() != RuleType.COMPOSITE) {
                compiled.put(definition.id().trim(), CompiledRule.compile(definition));
            }
        }
        for (RuleDefinition definition : copy) {
            if (definition.type() == RuleType.COMPOSITE) {
                resolveComposite(definition, byId, compiled, new HashSet<>());
            }
        }
        List<CompiledRule> ordered = new ArrayList<>(compiled.values());
        long seq = versionSequence.incrementAndGet();
        String version = "v" + seq;
        CompiledRuleSet snapshot = new CompiledRuleSet(new RuleSet(version, copy), List.copyOf(ordered));
        current.set(snapshot);
        return snapshot;
    }

    private CompiledRule resolveComposite(RuleDefinition definition,
                                          Map<String, RuleDefinition> byId,
                                          Map<String, CompiledRule> compiled,
                                          Set<String> resolving) {
        String id = definition.id().trim();
        CompiledRule existing = compiled.get(id);
        if (existing != null) {
            return existing;
        }
        if (!resolving.add(id)) {
            throw new InvalidRuleException("COMPOSITE_CYCLE",
                    "组合规则引用成环，环上包含规则: " + id);
        }
        String[] refs = definition.params().get("rules").trim().split(",");
        List<CompiledRule> children = new ArrayList<>();
        for (String refRaw : refs) {
            String ref = refRaw.trim();
            RuleDefinition childDef = byId.get(ref);
            if (childDef == null) {
                throw new InvalidRuleException("COMPOSITE_REF_MISSING",
                        "组合规则 " + id + " 引用了不存在的规则: " + ref);
            }
            if (childDef.type() == RuleType.COMPOSITE) {
                children.add(resolveComposite(childDef, byId, compiled, resolving));
            } else {
                children.add(compiled.get(ref));
            }
        }
        resolving.remove(id);
        CompiledRule rule = CompiledRule.composite(definition, children);
        compiled.put(id, rule);
        return rule;
    }
}
