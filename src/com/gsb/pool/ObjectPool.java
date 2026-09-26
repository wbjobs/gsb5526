package com.gsb.pool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 带泄漏检测的通用对象池（JDK 8，仅标准库）。
 *
 * 容量账目模型：
 *   池内部维护一个许可数为 maxCapacity 的公平信号量。任何线程必须先拿到一个许可，
 *   才能复用空闲对象或创建新对象；对象被借出期间许可一直被持有。
 *   因此不变式为：已借出数 + 借出在途数 <= maxCapacity，
 *   正在 borrow 中等待的线程排在信号量队列里，不占用许可也不会让账目超发。
 *   许可只在 release 的成功路径上归还；release 抛 IllegalStateException 时
 *   不触碰任何账目。
 *
 * 泄漏检测：
 *   后台守护线程周期扫描 + leaks()/borrow()/release() 调用时惰性扫描，
 *   借出时长超过 leakThresholdMillis 的对象生成一条 LeakReport（含借出堆栈
 *   和借出时间），同一笔借出只报告一次。
 */
public final class ObjectPool<T> {

    private final ObjectFactory<T> factory;
    private final Validator<T> validator;
    private final int maxCapacity;
    private final long leakThresholdNanos;

    private final Semaphore permits;

    private final Object lock = new Object();
    private final Deque<T> idle = new ArrayDeque<T>();
    private final Map<T, BorrowRecord> borrowed = new IdentityHashMap<T, BorrowRecord>();
    private final List<LeakReport> leakReports = new ArrayList<LeakReport>();

    private volatile boolean closed;
    private final Thread leakSweeper;
    private final long sweepIntervalMillis;

    private static final class BorrowRecord {
        final long borrowNanos;
        final long borrowTimeMillis;
        final String threadName;
        final StackTraceElement[] stackTrace;
        boolean reported;

        BorrowRecord() {
            this.borrowNanos = System.nanoTime();
            this.borrowTimeMillis = System.currentTimeMillis();
            this.threadName = Thread.currentThread().getName();
            this.stackTrace = Thread.currentThread().getStackTrace();
        }
    }

    /**
     * @param factory             对象工厂（create / destroy）
     * @param validator           对象校验器
     * @param maxCapacity         最大容量（任意时刻借出数上限），必须 >= 1
     * @param leakThresholdMillis 泄漏判定时间（毫秒），必须 >= 1
     */
    public ObjectPool(ObjectFactory<T> factory,
                      Validator<T> validator,
                      int maxCapacity,
                      long leakThresholdMillis) {
        if (factory == null || validator == null) {
            throw new NullPointerException("factory and validator must not be null");
        }
        if (maxCapacity < 1) {
            throw new IllegalArgumentException("maxCapacity must be >= 1");
        }
        if (leakThresholdMillis < 1) {
            throw new IllegalArgumentException("leakThresholdMillis must be >= 1");
        }
        this.factory = factory;
        this.validator = validator;
        this.maxCapacity = maxCapacity;
        this.leakThresholdNanos = TimeUnit.MILLISECONDS.toNanos(leakThresholdMillis);
        this.permits = new Semaphore(maxCapacity, true);
        this.sweepIntervalMillis = Math.min(1000L, Math.max(10L, leakThresholdMillis / 2));
        this.leakSweeper = new Thread(new Runnable() {
            @Override
            public void run() {
                sweepLoop();
            }
        }, "object-pool-leak-sweeper");
        this.leakSweeper.setDaemon(true);
        this.leakSweeper.start();
    }

    /**
     * 借出一个对象。
     *
     * @param timeoutMillis 最长等待时间；<= 0 表示不等待
     * @return 借到的对象；超时未获得返回 null；线程被中断时恢复中断标记并返回 null
     * @throws IllegalStateException 池已关闭
     */
    public T borrow(long timeoutMillis) {
        if (closed) {
            throw new IllegalStateException("pool is closed");
        }
        boolean acquired;
        try {
            if (timeoutMillis > 0) {
                acquired = permits.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                acquired = permits.tryAcquire();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        if (!acquired) {
            return null;
        }
        // 从此刻起持有一个许可，任何失败路径都必须归还它。
        try {
            T obj = takeIdleOrNull();
            if (obj != null && !isValidQuietly(obj)) {
                destroyQuietly(obj);
                obj = null;
            }
            if (obj == null) {
                obj = factory.create();
                if (obj == null) {
                    throw new IllegalStateException("factory.create() returned null");
                }
            }
            registerBorrow(obj);
            return obj;
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        } catch (Error e) {
            permits.release();
            throw e;
        }
    }

    /**
     * 归还一个对象。
     *
     * @throws IllegalStateException 对象不属于本池、或已被归还（重复 release）。
     *         抛出该异常时池的容量账目不发生任何变化。
     */
    public void release(T obj) {
        if (obj == null) {
            throw new IllegalStateException("cannot release null");
        }
        synchronized (lock) {
            // 关键：先从 borrowed 中移除。移除失败说明是外来对象或重复归还，
            // 此时直接抛异常，许可、idle、borrowed 一概不动。
            BorrowRecord rec = borrowed.remove(obj);
            if (rec == null) {
                throw new IllegalStateException(
                        "object was not borrowed from this pool or has already been released: " + obj);
            }
        }
        boolean returnToIdle = false;
        if (!closed && isValidQuietly(obj)) {
            synchronized (lock) {
                if (!closed) {
                    idle.addLast(obj);
                    returnToIdle = true;
                }
            }
        }
        if (!returnToIdle) {
            destroyQuietly(obj);
        }
        permits.release();
        scanForLeaks();
    }

    /**
     * 关闭池：销毁全部空闲对象；已借出的对象在归还时会被销毁而不是回到池中。
     * 关闭后 borrow 抛 IllegalStateException。可重复调用。
     */
    public void close() {
        List<T> toDestroy;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            toDestroy = new ArrayList<T>(idle);
            idle.clear();
        }
        leakSweeper.interrupt();
        for (T obj : toDestroy) {
            destroyQuietly(obj);
        }
    }

    /**
     * @return 当前全部泄漏报告的快照（按发现时间排序），调用方修改返回值不影响池。
     */
    public List<LeakReport> leaks() {
        scanForLeaks();
        synchronized (lock) {
            return new ArrayList<LeakReport>(leakReports);
        }
    }

    /** 当前已借出且未归还的对象数，主要用于监控和测试。 */
    public int borrowedCount() {
        synchronized (lock) {
            return borrowed.size();
        }
    }

    public int idleCount() {
        synchronized (lock) {
            return idle.size();
        }
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    // ------------------------------------------------------------------

    private T takeIdleOrNull() {
        synchronized (lock) {
            return idle.pollFirst();
        }
    }

    private void registerBorrow(T obj) {
        synchronized (lock) {
            if (closed) {
                // close 与 borrow 并发：对象不能借出，销毁并经由异常路径归还许可。
                destroyQuietly(obj);
                throw new IllegalStateException("pool is closed");
            }
            borrowed.put(obj, new BorrowRecord());
        }
        scanForLeaks();
    }

    private void scanForLeaks() {
        long now = System.nanoTime();
        List<LeakReport> fresh = null;
        synchronized (lock) {
            for (Map.Entry<T, BorrowRecord> entry : borrowed.entrySet()) {
                BorrowRecord rec = entry.getValue();
                if (!rec.reported && now - rec.borrowNanos >= leakThresholdNanos) {
                    rec.reported = true;
                    if (fresh == null) {
                        fresh = new ArrayList<LeakReport>();
                    }
                    fresh.add(new LeakReport(entry.getKey(), rec.borrowTimeMillis,
                            System.currentTimeMillis(), rec.threadName, rec.stackTrace));
                }
            }
            if (fresh != null) {
                leakReports.addAll(fresh);
            }
        }
    }

    private void sweepLoop() {
        while (!closed) {
            try {
                Thread.sleep(sweepIntervalMillis);
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
            }
            scanForLeaks();
        }
    }

    private boolean isValidQuietly(T obj) {
        try {
            return validator.isValid(obj);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void destroyQuietly(T obj) {
        try {
            factory.destroy(obj);
        } catch (RuntimeException e) {
            // 销毁失败不影响池账目。
        }
    }
}
