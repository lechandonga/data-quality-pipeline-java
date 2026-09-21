package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.FileBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 文件基线存储：持久化往返、失败不破坏旧基线、文件名清洗。
 */
class FileBaselineStoreTest {

    @Test
    void commitAndReloadRoundTrip(@TempDir Path dir) {
        FileBaselineStore store = new FileBaselineStore(dir);
        Baseline b = new Baseline("amount", 3, 300, 290, 280, 42.5, 12.25, 200);
        store.commit(b);

        FileBaselineStore reloaded = new FileBaselineStore(dir);
        Optional<Baseline> loaded = reloaded.load("amount");
        assertTrue(loaded.isPresent());
        assertEquals(b, loaded.get());
    }

    @Test
    void missingFieldReturnsEmpty(@TempDir Path dir) {
        FileBaselineStore store = new FileBaselineStore(dir);
        assertTrue(store.load("never-seen").isEmpty());
    }

    @Test
    void commitFailureLeavesOldFileIntact(@TempDir Path dir) {
        FileBaselineStore good = new FileBaselineStore(dir);
        Baseline old = new Baseline("amount", 1, 10, 10, 10, 1.0, 0.0, 10);
        good.commit(old);

        // 将目录设为只读，使后续提交（写临时文件）真实失败
        dir.toFile().setWritable(false);
        try {
            FileBaselineStore failing = new FileBaselineStore(dir);
            Baseline newer = new Baseline("amount", 2, 20, 20, 20, 2.0, 0.0, 20);
            assertThrows(PipelineException.class, () -> failing.commit(newer));
        } finally {
            dir.toFile().setWritable(true);
        }

        // 重新从磁盘加载：仍然是旧基线
        FileBaselineStore check = new FileBaselineStore(dir);
        assertEquals(old, check.load("amount").orElseThrow());
    }

    @Test
    void corruptedFileRaisesDistinctError(@TempDir Path dir) throws Exception {
        FileBaselineStore store = new FileBaselineStore(dir);
        Path file = dir.resolve("baseline-bad.tsv");
        Files.writeString(file, "only\tthree\n");
        PipelineException ex = assertThrows(PipelineException.class, () -> store.load("bad"));
        assertEquals("BASELINE_IO_ERROR", ex.reasonCode());
    }

    @Test
    void fieldNameSanitized(@TempDir Path dir) {
        FileBaselineStore store = new FileBaselineStore(dir);
        Baseline b = new Baseline("a.b/c", 1, 1, 1, 1, 1, 0, 1);
        store.commit(b);
        assertTrue(Files.exists(dir.resolve("baseline-a.b_c.tsv")));
    }
}
