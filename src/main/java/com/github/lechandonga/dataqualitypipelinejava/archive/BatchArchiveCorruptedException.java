package com.github.lechandonga.dataqualitypipelinejava.archive;

/**
 * 磁盘上的批次存档存在但无法完整解析（损坏/截断/缺字段）。
 * 必须显式抛出，绝不允许静默当成"批次不存在"。
 */
public class BatchArchiveCorruptedException extends RuntimeException {

    public BatchArchiveCorruptedException(String message) {
        super(message);
    }

    public BatchArchiveCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
