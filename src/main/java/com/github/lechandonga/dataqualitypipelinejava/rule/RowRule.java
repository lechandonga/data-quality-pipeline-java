package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;

import java.util.List;

/**
 * 单条记录级别的无状态校验规则。实现必须是线程安全、纯函数式的，
 * 同一条记录在任何线程、任何批次下得到相同结论。
 */
public interface RowRule {

    /** 规则定义中的唯一 id。 */
    String id();

    /** 评估一条记录，返回空列表表示通过；失败原因须可定位字段。 */
    List<FailureReason> evaluate(DataRecord record);
}
