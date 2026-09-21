package com.github.lechandonga.dataqualitypipelinejava.stream;

import com.github.lechandonga.dataqualitypipelinejava.engine.CapacityExceededException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 有界阻塞记录队列：大输入流水线的明确容量边界。
 *
 * <ul>
 *   <li>容量固定，生产者在队列满时被阻塞（背压），不会无界占用内存；</li>
 *   <li>入队等待超过 offerTimeoutMillis 时抛出 {@link CapacityExceededException}；</li>
 *   <li>{@link #close()} 可靠投递唯一结束哨兵（队列满也阻塞等待空位），消费者据此结束；</li>
 *   <li>{@link #abort()} 用于消费者致命错误时的协作关闭：立即唤醒所有阻塞的
 *       生产者与消费者，双方都不会再互等。</li>
 * </ul>
 */
public final class BoundedRecordQueue {

    /** 哨兵使用私有 new 出来的实例，保证身份比较绝不与任何用户记录冲突。 */
    private static final Map<String, String> POISON = new HashMap<>();

    private final BlockingQueue<Map<String, String>> queue;
    private final long offerTimeoutMillis;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    public BoundedRecordQueue(int capacity) {
        this(capacity, 5_000L);
    }

    public BoundedRecordQueue(int capacity, long offerTimeoutMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("队列容量必须为正数: " + capacity);
        }
        if (offerTimeoutMillis <= 0) {
            throw new IllegalArgumentException("offerTimeoutMillis 必须为正数");
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.offerTimeoutMillis = offerTimeoutMillis;
    }

    public void put(Map<String, String> record) throws InterruptedException {
        if (aborted.get()) {
            throw new CapacityExceededException("批次已中止，停止投入记录");
        }
        if (closed.get()) {
            throw new CapacityExceededException("队列已关闭，不能再投入记录");
        }
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(offerTimeoutMillis);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new CapacityExceededException(
                        "记录队列在 " + offerTimeoutMillis + "ms 内仍无空位，按容量策略拒绝");
            }
            if (queue.offer(record, Math.min(remaining, 50L), TimeUnit.MILLISECONDS)) {
                return;
            }
            if (aborted.get()) {
                throw new CapacityExceededException("批次已中止，停止投入记录");
            }
            if (closed.get()) {
                throw new CapacityExceededException("队列已关闭，不能再投入记录");
            }
        }
    }

    /**
     * 取下一条记录；无记录时阻塞。返回 null 表示流已结束（收到哨兵或已中止）。
     */
    public Map<String, String> poll() throws InterruptedException {
        while (true) {
            Map<String, String> record = queue.poll(50, TimeUnit.MILLISECONDS);
            if (record == POISON) {
                return null;
            }
            if (record != null) {
                return record;
            }
            if (aborted.get()) {
                return null;
            }
        }
    }

    /**
     * 幂等关闭：保证哨兵一定被消费者收到（队列满时阻塞等待空位）。
     */
    public void close() {
        if (closed.compareAndSet(false, true) && !aborted.get()) {
            try {
                queue.put(POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CapacityExceededException("投递结束哨兵时被中断");
            }
        }
    }

    /**
     * 中止批次：唤醒阻塞的生产者/消费者，结束哨兵不再需要。
     */
    public void abort() {
        if (aborted.compareAndSet(false, true)) {
            queue.clear();
            queue.offer(POISON);
        }
    }

    public int size() {
        return queue.size();
    }
}
