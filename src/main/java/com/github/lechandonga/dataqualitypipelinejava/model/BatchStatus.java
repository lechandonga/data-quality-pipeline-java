package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 批次生命周期状态。
 */
public enum BatchStatus {
    RUNNING,
    COMMITTED,
    REJECTED,
    FAILED,
    ISOLATED
}
