package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.FileBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.model.FieldStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileBaselineStoreTest {

    private Baseline baseline(long batchCount, double mean) {
        Map<String, FieldStats> stats = new LinkedHashMap<>();
        stats.put("v", new FieldStats(10, 1, mean, 4.0, 0.0, 20.0, 9));
        return new Baseline("ds-1", batchCount, stats);
    }

    @Test
    void saveAndLoadRoundTripAcrossInstances(@TempDir Path dir) {
        new FileBaselineStore(dir.toString()).save("ds-1", baseline(3, 11.5));
        // 模拟重启：新实例从同一目录加载
        Baseline loaded = new FileBaselineStore(dir.toString()).load("ds-1").orElseThrow();
        assertEquals(3, loaded.batchCount());
        assertEquals(11.5, loaded.fieldStats().get("v").mean(), 1e-12);
        assertEquals(4.0, loaded.fieldStats().get("v").variance(), 1e-12);
        assertEquals(9, loaded.fieldStats().get("v").distinct());
    }

    @Test
    void missingDatasetLoadsEmpty(@TempDir Path dir) {
        assertTrue(new FileBaselineStore(dir.toString()).load("nope").isEmpty());
    }

    @Test
    void atomicWriteLeavesNoTempFiles(@TempDir Path dir) throws IOException {
        FileBaselineStore store = new FileBaselineStore(dir.toString());
        for (int i = 0; i < 10; i++) {
            store.save("ds-1", baseline(i + 1, i));
        }
        long tmpFiles = Files.list(dir).filter(f -> f.toString().endsWith(".tmp")).count();
        assertEquals(0, tmpFiles, "temp files must be renamed away atomically");
        assertEquals(10, store.load("ds-1").orElseThrow().batchCount());
    }

    @Test
    void datasetsAreStoredInSeparateFiles(@TempDir Path dir) {
        FileBaselineStore store = new FileBaselineStore(dir.toString());
        store.save("ds-1", baseline(1, 1));
        store.save("ds-2", baseline(2, 2));
        assertEquals(1, store.load("ds-1").orElseThrow().batchCount());
        assertEquals(2, store.load("ds-2").orElseThrow().batchCount());
    }

    @Test
    void corruptedBaselineIsRejectedNotSilentlyReset(@TempDir Path dir) throws IOException {
        FileBaselineStore store = new FileBaselineStore(dir.toString());
        store.save("ds-1", baseline(1, 1));
        Path file = dir.resolve("ds-1.baseline.json");
        Files.writeString(file, "{ this is not valid json ");
        assertThrows(UncheckedIOException.class, () -> store.load("ds-1"),
                "corrupted baseline must fail loudly rather than being treated as absent");
    }
}
