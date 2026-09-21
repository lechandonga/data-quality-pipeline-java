package com.github.lechandonga.dataqualitypipelinejava.config;

/**
 * 达到容量上限时的策略。
 */
public enum OverflowStrategy {
    /** 拒绝整个批次（抛出 BatchRejectedException），不更新基线 */
    REJECT,
    /** 超出上限的记录进入隔离区，批次标记为 ISOLATED，不更新基线 */
    ISOLATE
}
