package com.github.lechandonga.dataqualitypipelinejava.baseline;

/**
 * 基线原子更新失败时抛出。此时批次不会被标记为 COMMITTED，旧基线保持不变。
 */
public class BaselineCommitException extends RuntimeException {

    public BaselineCommitException(String message, Throwable cause) {
        super(message, cause);
    }
}
