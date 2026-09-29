package com.github.lechandonga.dataqualitypipelinejava.archive;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 基于本地文件系统的批次存档：每个批次一个 JSON 文件。
 *
 * 写入采用"临时文件 + 原子 move"：要么完整新快照可见，要么旧快照原样保留，
 * 中途失败/被打断不会留下半个批次的残档。读取时严格校验结构与关键不变量，
 * 任何损坏（截断、JSON 非法、字段缺失、计数与条目不一致）都显式抛
 * {@link ArchiveException}，绝不静默当成"批次不存在"。
 */
public class FileBatchArchive implements BatchArchive {

    static final int FORMAT_VERSION = 1;

    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Path directory;

    /** 按目标文件分条加锁：同批次并发写串行化，避免并发非原子 move 互相截断。 */
    private final ConcurrentMap<Path, Object> saveLocks = new ConcurrentHashMap<>();

    public FileBatchArchive(String directory) {
        this.directory = Path.of(directory);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    private Path target(String batchId) {
        return directory.resolve(safe(batchId) + ".batch.json");
    }

    private String safe(String batchId) {
        return batchId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    @Override
    public void save(BatchSnapshot snapshot) {
        Path target = target(snapshot.batchId());
        Object lock = saveLocks.computeIfAbsent(target, k -> new Object());
        synchronized (lock) {
            // 临时文件在锁内取唯一名（线程 + 纳秒），同批次并发保存互不覆盖
            Path tmp = directory.resolve(safe(snapshot.batchId()) + ".batch."
                    + Thread.currentThread().threadId() + "-" + System.nanoTime() + ".tmp");
            try {
                Files.createDirectories(directory);
                Map<String, Object> envelope = new LinkedHashMap<>();
                envelope.put("formatVersion", FORMAT_VERSION);
                envelope.put("snapshot", snapshot);
                byte[] payload = mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsBytes(envelope);
                Files.write(tmp, payload);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException atomicUnsupported) {
                    // 文件系统不支持原子 move 时退化为普通 move；同批次写入已被
                    // 上面的锁串行化，因此不会出现两个 move 互相截断目标的中间态。
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                // 写失败：清理临时文件，绝不允许残档被误读；已存在的旧快照不动。
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // best-effort 清理
                }
                throw new ArchiveException("failed to persist batch archive: "
                        + snapshot.batchId(), e);
            }
        }
    }

    @Override
    public Optional<BatchSnapshot> load(String batchId) {
        Path file = target(batchId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        BatchSnapshot snapshot;
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == 0) {
                throw new ArchiveException("batch archive file is empty: " + file);
            }
            String json = new String(bytes, StandardCharsets.UTF_8);
            Envelope envelope;
            try {
                envelope = mapper.readValue(json, Envelope.class);
            } catch (IOException e) {
                throw new ArchiveException("corrupted batch archive (unreadable JSON): " + file, e);
            }
            snapshot = envelope.snapshot;
        } catch (IOException e) {
            throw new ArchiveException("failed to read batch archive: " + file, e);
        }
        validate(file, snapshot);
        return Optional.of(snapshot);
    }

    /** 存档结构与关键不变量校验：任何异常都按"损坏"显式报错。 */
    private void validate(Path file, BatchSnapshot snapshot) {
        if (snapshot == null || snapshot.result() == null) {
            throw new ArchiveException("corrupted batch archive (missing result): " + file);
        }
        if (snapshot.records() == null) {
            throw new ArchiveException("corrupted batch archive (missing records): " + file);
        }
        if (snapshot.replayProps() == null) {
            throw new ArchiveException("corrupted batch archive (missing replay properties): " + file);
        }
        var result = snapshot.result();
        if (result.violations() == null) {
            throw new ArchiveException("corrupted batch archive (missing violations): " + file);
        }
        if (result.violations().size() != snapshot.records().size()) {
            throw new ArchiveException("corrupted batch archive (record/verdict count mismatch): "
                    + file);
        }
        long verdicts = snapshot.records().size();
        if (result.total() != verdicts) {
            throw new ArchiveException("corrupted batch archive (total mismatch): " + file);
        }
        for (ArchivedRecord record : snapshot.records()) {
            if (record.recordId() == null || record.violation() == null) {
                throw new ArchiveException("corrupted batch archive (malformed record): " + file);
            }
            if (!record.recordId().equals(record.violation().recordId())) {
                throw new ArchiveException("corrupted batch archive (record id mismatch): " + file);
            }
        }
    }

    /** 外层信封，携带存档格式版本。 */
    public static final class Envelope {
        public int formatVersion;
        public BatchSnapshot snapshot;

        public Envelope() {
        }

        public int getFormatVersion() {
            return formatVersion;
        }

        public void setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
        }

        public BatchSnapshot getSnapshot() {
            return snapshot;
        }

        public void setSnapshot(BatchSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
