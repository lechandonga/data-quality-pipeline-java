package com.github.lechandonga.dataqualitypipelinejava.archive;

/**
 * 存档损坏或无法读取时抛出。必须显式失败：不能把损坏存档静默当成"批次不存在"。
 */
public class ArchiveException extends RuntimeException {

    public ArchiveException(String message) {
        super(message);
    }

    public ArchiveException(String message, Throwable cause) {
        super(message, cause);
    }
}
