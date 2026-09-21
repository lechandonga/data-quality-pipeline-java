package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地文件基线存储：每个字段一个 TSV 文件，
 * 通过“写临时文件 + 原子 move”保证提交要么整体生效、要么完全不生效，
 * 半写入（进程崩溃、磁盘故障）绝不会留下损坏基线；并发提交按字段串行。
 *
 * <p>内存中保留缓存以加速 load；首次访问惰性加载。</p>
 */
public final class FileBaselineStore implements BaselineStore {

    private static final Logger log = LoggerFactory.getLogger(FileBaselineStore.class);

    private final Path directory;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Optional<Baseline>> cache = new ConcurrentHashMap<>();

    public FileBaselineStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new PipelineException("BASELINE_STORAGE_INIT_FAILED",
                    "无法创建基线目录 " + directory + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Baseline> load(String field) {
        return cache.computeIfAbsent(field, this::readFromDisk);
    }

    @Override
    public void prepareCommit(Baseline next) {
        if (next == null || next.field() == null) {
            throw new IllegalArgumentException("预检的基线不能为空且必须包含字段名");
        }
        // 仅在目录内创建并立即删除探针文件，不触碰任何已有基线；
        // 提前暴露只读目录/磁盘满等问题，避免多字段提交到一半才失败。
        Path probe = directory.resolve(".probe-" + sanitize(next.field()) + "-"
                + Thread.currentThread().getId());
        try {
            Files.writeString(probe, "ok", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new PipelineException("BASELINE_IO_ERROR",
                    "基线字段 " + next.field() + " 提交预检失败: " + e.getMessage(), e);
        } finally {
            safeDelete(probe);
        }
    }

    @Override
    public void rollback(java.util.List<Baseline> previous) {
        for (Baseline old : previous) {
            writeAtomically(fileFor(old.field()), serialize(old));
            cache.put(old.field(), Optional.of(old));
        }
    }

    @Override
    public void commit(Baseline next) {
        if (next == null || next.field() == null) {
            throw new IllegalArgumentException("提交的基线不能为空且必须包含字段名");
        }
        ReentrantLock lock = locks.computeIfAbsent(next.field(), k -> new ReentrantLock());
        lock.lock();
        try {
            writeAtomically(fileFor(next.field()), serialize(next));
            cache.put(next.field(), Optional.of(next));
            log.debug("基线字段[{}]已提交, 累计批次={}", next.field(), next.batchCount());
        } finally {
            lock.unlock();
        }
    }

    private void writeAtomically(Path target, String content) {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp-"
                + Thread.currentThread().getId());
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8);
            // 原子替换：崩溃只可能发生在 move 之前（旧文件完好）或之后（新文件完整）
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailed) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            safeDelete(tmp);
            throw new PipelineException("BASELINE_IO_ERROR",
                    "基线文件 " + target + " 写入失败，已有基线保持不变: "
                            + e.getMessage(), e);
        }
    }

    private Optional<Baseline> readFromDisk(String field) {
        Path target = fileFor(field);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try (BufferedReader reader = Files.newBufferedReader(target, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            if (line == null) {
                return Optional.empty();
            }
            return Optional.of(deserialize(field, line));
        } catch (IOException | RuntimeException e) {
            throw new PipelineException("BASELINE_IO_ERROR",
                    "基线字段 " + field + " 读取失败: " + e.getMessage(), e);
        }
    }

    private Path fileFor(String field) {
        return directory.resolve("baseline-" + sanitize(field) + ".tsv");
    }

    private static String sanitize(String field) {
        return field.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }

    private static String serialize(Baseline b) {
        return String.join("\t",
                b.field(),
                Long.toString(b.batchCount()),
                Long.toString(b.totalCount()),
                Long.toString(b.nonNullCount()),
                Long.toString(b.numericCount()),
                Double.toString(b.mean()),
                Double.toString(b.m2()),
                Long.toString(b.distinctCount()))
                + System.lineSeparator();
    }

    private static Baseline deserialize(String field, String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length != 8) {
            throw new IllegalStateException("基线文件列数应为 8，实际 " + parts.length);
        }
        return new Baseline(
                field,
                Long.parseLong(parts[1]),
                Long.parseLong(parts[2]),
                Long.parseLong(parts[3]),
                Long.parseLong(parts[4]),
                Double.parseDouble(parts[5]),
                Double.parseDouble(parts[6]),
                Long.parseLong(parts[7]));
    }

    private static void safeDelete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // 临时文件清理失败不影响正确性
        }
    }
}
