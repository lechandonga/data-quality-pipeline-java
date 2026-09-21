package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;
import com.github.lechandonga.dataqualitypipelinejava.rule.RowRule;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * 测试用：前 N 条记录的评估在屏障上等待，确定性地把 worker 全部占住，
 * 使摄入线程必然打满有界队列（与机器速度无关）。
 */
final class BarrierRule implements RowRule {

    private final String id;
    private final CyclicBarrier barrier;
    private final int blockedCount;
    private int seen;

    BarrierRule(String id, CyclicBarrier barrier, int blockedCount) {
        this.id = id;
        this.barrier = barrier;
        this.blockedCount = blockedCount;
    }

    @Override
    public synchronized String id() {
        return id;
    }

    @Override
    public List<FailureReason> evaluate(DataRecord record) {
        boolean wait;
        synchronized (this) {
            wait = seen++ < blockedCount;
        }
        if (wait) {
            try {
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return List.of();
    }
}
