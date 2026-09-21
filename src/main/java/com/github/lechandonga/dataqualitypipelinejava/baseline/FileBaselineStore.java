package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.model.FieldStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 基于本地文件系统的基线存储。
 * 写入采用"临时文件 + 原子 move"：要么新基线完整可见，要么旧基线保持不变，
 * 不会留下部分写入的损坏文件。损坏文件将被显式拒绝而非静默重置。
 */
public class FileBaselineStore implements BaselineStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path directory;

    public FileBaselineStore(String directory) {
        this.directory = Path.of(directory);
    }

    private Path target(String dataset) {
        return directory.resolve(safe(dataset) + ".baseline.json");
    }

    private String safe(String dataset) {
        return dataset.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    @Override
    public synchronized Optional<Baseline> load(String dataset) {
        Path file = target(dataset);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == 0) {
                throw new UncheckedIOException(new IOException("baseline file is empty: " + file));
            }
            Map<String, Object> root = MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {
            });
            return Optional.of(deserialize(dataset, root));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read baseline: " + file, e);
        }
    }

    @Override
    public synchronized void save(String dataset, Baseline baseline) {
        try {
            Files.createDirectories(directory);
            Path target = target(dataset);
            Path tmp = directory.resolve(safe(dataset) + ".baseline."
                    + Thread.currentThread().threadId() + ".tmp");
            byte[] payload = MAPPER.writeValueAsBytes(serialize(baseline));
            Files.write(tmp, payload);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist baseline for dataset=" + dataset, e);
        }
    }

    private Map<String, Object> serialize(Baseline b) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("dataset", b.dataset());
        root.put("batchCount", b.batchCount());
        Map<String, Object> stats = new LinkedHashMap<>();
        b.fieldStats().forEach((f, s) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("count", s.count());
            m.put("nullCount", s.nullCount());
            m.put("mean", s.mean());
            m.put("variance", s.variance());
            m.put("min", s.min());
            m.put("max", s.max());
            m.put("distinct", s.distinct());
            stats.put(f, m);
        });
        root.put("fieldStats", stats);
        return root;
    }

    private Baseline deserialize(String dataset, Map<String, Object> root) {
        long batchCount = ((Number) root.getOrDefault("batchCount", 0)).longValue();
        Object rawStats = root.get("fieldStats");
        Map<String, FieldStats> stats = new LinkedHashMap<>();
        if (rawStats instanceof Map<?, ?> map) {
            map.forEach((k, v) -> {
                Map<?, ?> m = (Map<?, ?>) v;
                stats.put((String) k, new FieldStats(
                        num(m, "count"), num(m, "nullCount"),
                        dbl(m, "mean"), dbl(m, "variance"),
                        dbl(m, "min"), dbl(m, "max"), num(m, "distinct")));
            });
        }
        return new Baseline(dataset, batchCount, stats);
    }

    private long num(Map<?, ?> m, String key) {
        Object v = m.get(key);
        return v == null ? 0L : ((Number) v).longValue();
    }

    private double dbl(Map<?, ?> m, String key) {
        Object v = m.get(key);
        return v == null ? 0.0 : ((Number) v).doubleValue();
    }
}
