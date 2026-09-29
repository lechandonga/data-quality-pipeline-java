package com.github.lechandonga.dataqualitypipelinejava.archive;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * 基于本地目录的批次存档。
 *
 * <p>写入路径固定为"完整序列化为字节 → 写同目录临时文件 → 原子 move 替换"：
 * 中途失败或进程被打断时，正式文件名要么不存在、要么指向之前的完整快照，
 * 不会留下半个批次的残档；临时残档会被清理。
 *
 * <p>读取时，正式文件存在但为空、JSON 损坏、缺关键字段，一律抛出
 * {@link BatchArchiveCorruptedException}，显式区分于"批次从未存档"。
 */
public class FileBatchArchiveStore implements BatchArchiveStore {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true)
            .configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);

    private final Path directory;
    private volatile boolean failNextSave = false;

    public FileBatchArchiveStore(String directory) {
        this.directory = Path.of(directory);
    }

    private Path target(String batchId) {
        String safe = batchId.replaceAll("[^a-zA-Z0-9._-]", "_");
        // 附原始 id 的稳定哈希，避免不同 id 被清洗后发生文件名碰撞
        String suffix = Integer.toHexString(batchId.hashCode());
        return directory.resolve(safe + "-" + suffix + ".batch.json");
    }

    @Override
    public Optional<BatchSnapshot> load(String batchId) {
        Path file = target(batchId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new BatchArchiveCorruptedException("failed to read batch archive: " + file, e);
        }
        if (bytes.length == 0) {
            throw new BatchArchiveCorruptedException("batch archive is empty: " + file);
        }
        BatchSnapshot snapshot;
        try {
            snapshot = MAPPER.readValue(bytes, BatchSnapshot.class);
        } catch (IOException e) {
            throw new BatchArchiveCorruptedException(
                    "batch archive is corrupted or truncated: " + file, e);
        }
        validate(file, snapshot);
        return Optional.of(snapshot);
    }

    private void validate(Path file, BatchSnapshot snapshot) {
        if (snapshot == null
                || snapshot.batchId() == null
                || snapshot.dataset() == null
                || snapshot.status() == null
                || snapshot.rulesVersion() == null
                || snapshot.entries() == null
                || snapshot.driftFindings() == null) {
            throw new BatchArchiveCorruptedException(
                    "batch archive missing required fields: " + file);
        }
        snapshot.entries().forEach(e -> {
            if (e == null || e.recordId() == null || e.status() == null
                    || e.data() == null || e.reasons() == null) {
                throw new BatchArchiveCorruptedException(
                        "batch archive contains a malformed record entry: " + file);
            }
        });
    }

    @Override
    public void save(BatchSnapshot snapshot) {
        if (failNextSave) {
            failNextSave = false;
            throw new BatchArchiveException(
                    "injected archive write failure for batch=" + snapshot.batchId(),
                    new IOException("simulated disk failure"));
        }
        Path target = target(snapshot.batchId());
        Path tmp = directory.resolve(target.getFileName() + "."
                + Thread.currentThread().threadId() + ".tmp");
        try {
            Files.createDirectories(directory);
            byte[] payload = MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(snapshot);
            // 先把完整字节落到临时文件，再用 move 一步切换可见性
            Files.write(tmp, payload);
            try {
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException atomicUnsupported) {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException moveFailed) {
                deleteQuietly(tmp);
                throw moveFailed;
            }
        } catch (IOException e) {
            deleteQuietly(tmp);
            throw new BatchArchiveException(
                    "failed to persist batch archive for batch=" + snapshot.batchId(), e);
        }
    }

    @Override
    public void armNextSaveFailure() {
        this.failNextSave = true;
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }

}
