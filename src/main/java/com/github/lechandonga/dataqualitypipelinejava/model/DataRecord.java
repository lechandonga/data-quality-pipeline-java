package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一条输入记录。data 为有序字段映射，保证迭代顺序与输入一致，
 * 从而使结论不依赖 HashMap 的非确定顺序。
 */
public final class DataRecord {

    private final String id;
    private final long sourceOrder;
    private final Map<String, String> data;

    public DataRecord(String id, long sourceOrder, Map<String, String> data) {
        this.id = id;
        this.sourceOrder = sourceOrder;
        this.data = new LinkedHashMap<>(data);
    }

    public String id() {
        return id;
    }

    public long sourceOrder() {
        return sourceOrder;
    }

    public Map<String, String> data() {
        return new LinkedHashMap<>(data);
    }

    public String field(String name) {
        return data.get(name);
    }

    /** 返回一个字段被覆盖/移除的新记录，不修改当前不可变记录。 */
    public DataRecord withField(String name, String value) {
        Map<String, String> copy = new LinkedHashMap<>(data);
        if (value == null) {
            copy.remove(name);
        } else {
            copy.put(name, value);
        }
        return new DataRecord(id, sourceOrder, copy);
    }
}
