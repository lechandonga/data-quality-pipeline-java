package com.github.lechandonga.dataqualitypipelinejava.archive;

/** 批次存档写入失败（磁盘、IO 等）。本次执行结论保留，标记为 NOT_PERSISTED。 */
public class BatchArchiveException extends RuntimeException {

    public BatchArchiveException(String message, Throwable cause) {
        super(message, cause);
    }
}
