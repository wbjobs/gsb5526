package com.gsb.pool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 无 JUnit 的自测程序：javac 编译后直接 java com.gsb.pool.SelfTest 运行。
 * 任一断言失败即抛 AssertionError 并以非零码退出。
 */
public final class SelfTest {

    private static int passed = 0;

    public static void main(String[] args) throws Exception {
        testCapacityNeverExceededUnderContention();
        testBorrowTimeoutReturnsNull();
        testDoubleReleaseThrowsAndKeepsAccounting();
        testForeignReleaseThrowsAndKeepsAccounting();
        testLeakReportedWithStackAndTime();
        testLeakReportedOnlyOnceAndSurvivesRelease();
        testCloseBehavior();
        testValidatorEvictsBadObjects();
        System.out.println("ALL " + passed + " TESTS PASSED");
    }

    /** 硬要求 1：高并发下任意时刻借出数不超过最大容量。 */
    static void testCapacityNeverExceededUnderContention() throws Exception {
        final int max = 4;
        final ObjectPool<Object> pool = newPool(max, 60_000L);
        final AtomicInteger inUse = new AtomicInteger();
        final AtomicInteger observedMax = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final int threads = 32;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        for (int j = 0; j < 200; j++) {
                            Object o = pool.borrow(5_000L);
                            check(o != null, "borrow must succeed within timeout");
                            int now = inUse.incrementAndGet();
                            int prev;
                            do {
                                prev = observedMax.get();
                            } while (now > prev && !observedMax.compareAndSet(prev, now));
                            check(now <= max, "borrowed count " + now + " exceeded max " + max);
                            check(pool.borrowedCount() <= max, "pool borrowedCount exceeded max");
                            Thread.sleep(1L);
                            inUse.decrementAndGet();
                            pool.release(o);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                }
            }).start();
        }
        start.countDown();
        check(done.await(60L, TimeUnit.SECONDS), "workers did not finish in time");
        check(failure.get() == null, "worker failed: " + failure.get());
        check(observedMax.get() == max, "expected pool to be fully utilized, got " + observedMax.get());
        check(pool.borrowedCount() == 0, "all objects must be returned");
        check(pool.idleCount() <= max, "idle must not exceed max");
        pool.close();
        passed++;
    }

    /** 容量占满后 borrow 在超时时间内拿不到对象必须返回 null。 */
    static void testBorrowTimeoutReturnsNull() {
        ObjectPool<Object> pool = newPool(1, 60_000L);
        Object o = pool.borrow(1_000L);
        check(o != null, "first borrow must succeed");
        long start = System.nanoTime();
        Object second = pool.borrow(150L);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        check(second == null, "second borrow must time out and return null");
        check(elapsedMs >= 100L, "must actually wait, waited " + elapsedMs + "ms");
        check(pool.borrow(0L) == null, "non-blocking borrow on exhausted pool must return null");
        pool.release(o);
        check(pool.borrow(0L) != null, "borrow after release must succeed");
        pool.close();
        passed++;
    }

    /** 硬要求 2：重复 release 抛 IllegalStateException，且账目分毫不动。 */
    static void testDoubleReleaseThrowsAndKeepsAccounting() {
        ObjectPool<Object> pool = newPool(2, 60_000L);
        Object a = pool.borrow(1_000L);
        Object b = pool.borrow(1_000L);
        pool.release(a);
        int borrowedBefore = pool.borrowedCount();
        int idleBefore = pool.idleCount();
        expectIllegalState(new Runnable() {
            @Override
            public void run() {
                // 重复归还同一个对象（a 已不在 borrowed 账上）
                pool.release(a);
            }
        });
        check(pool.borrowedCount() == borrowedBefore, "borrowed count must not change");
        check(pool.idleCount() == idleBefore, "idle count must not change");
        // 账目未被污染：容量仍是 2，b 占 1，只能再借 1 个，第 3 个必须超时
        check(pool.borrow(0L) != null, "one more slot must be available");
        check(pool.borrow(50L) == null, "pool must be exhausted at max capacity");
        pool.release(b);
        pool.close();
        passed++;
    }

    /** 硬要求 2：release 外来对象抛 IllegalStateException，且账目分毫不动。 */
    static void testForeignReleaseThrowsAndKeepsAccounting() {
        ObjectPool<Object> pool = newPool(1, 60_000L);
        ObjectPool<Object> otherPool = newPool(1, 60_000L);
        Object mine = pool.borrow(1_000L);
        final Object foreign = otherPool.borrow(1_000L);
        int borrowedBefore = pool.borrowedCount();
        int idleBefore = pool.idleCount();
        expectIllegalState(new Runnable() {
            @Override
            public void run() {
                pool.release(foreign);
            }
        });
        expectIllegalState(new Runnable() {
            @Override
            public void run() {
                pool.release(new Object());
            }
        });
        check(pool.borrowedCount() == borrowedBefore, "borrowed count must not change");
        check(pool.idleCount() == idleBefore, "idle count must not change");
        check(pool.borrow(50L) == null, "pool must still be exhausted (permit not leaked)");
        pool.release(mine);
        check(pool.borrow(0L) != null, "pool must still function after bad releases");
        pool.close();
        otherPool.close();
        passed++;
    }

    /** 硬要求 3：超时未还的对象生成泄漏报告，含借出堆栈与借出时间。 */
    static void testLeakReportedWithStackAndTime() throws Exception {
        ObjectPool<Object> pool = newPool(1, 150L);
        long before = System.currentTimeMillis();
        Object o = borrowViaNamedCallSite(pool); // 具名调用点，便于在堆栈里认出来
        long after = System.currentTimeMillis();
        Thread.sleep(800L); // 让后台 sweeper 有足够时间扫描
        List<LeakReport> leaks = pool.leaks();
        check(leaks.size() == 1, "expected exactly 1 leak report, got " + leaks.size());
        LeakReport report = leaks.get(0);
        check(report.getLeakedObject() == o, "report must reference the leaked object");
        check(report.getBorrowTimeMillis() >= before && report.getBorrowTimeMillis() <= after,
                "borrow time must be the actual borrow instant");
        check(report.getDetectedTimeMillis() >= report.getBorrowTimeMillis(),
                "detection must not precede borrow");
        StackTraceElement[] stack = report.getBorrowStackTrace();
        check(stack != null && stack.length > 0, "report must carry borrow-time stack");
        boolean foundCallSite = false;
        for (StackTraceElement el : stack) {
            if ("borrowViaNamedCallSite".equals(el.getMethodName())) {
                foundCallSite = true;
                break;
            }
        }
        check(foundCallSite, "stack must contain the borrow call site");
        check(report.getBorrowThreadName().equals(Thread.currentThread().getName()),
                "report must record the borrowing thread");
        pool.release(o);
        pool.close();
        passed++;
    }

    static Object borrowViaNamedCallSite(ObjectPool<Object> pool) {
        return pool.borrow(1_000L);
    }

    /** 同一笔借出只报告一次；归还后报告仍然保留。 */
    static void testLeakReportedOnlyOnceAndSurvivesRelease() throws Exception {
        ObjectPool<Object> pool = newPool(1, 120L);
        Object o = pool.borrow(1_000L);
        Thread.sleep(600L);
        check(pool.leaks().size() == 1, "leak must be reported");
        Thread.sleep(600L);
        check(pool.leaks().size() == 1, "same borrow must not be reported twice");
        pool.release(o); // 泄漏后归还，池必须正常工作
        check(pool.leaks().size() == 1, "report must survive the release");
        check(pool.borrowedCount() == 0, "release after leak must update accounting");
        Object again = pool.borrow(0L);
        check(again != null, "pool must remain usable");
        pool.release(again);
        pool.close();
        passed++;
    }

    /** close：空闲对象被销毁、借出对象归还时被销毁、关闭后 borrow 抛异常、可重复关闭。 */
    static void testCloseBehavior() {
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger destroyed = new AtomicInteger();
        ObjectPool<Object> pool = new ObjectPool<Object>(new ObjectFactory<Object>() {
            @Override
            public Object create() {
                created.incrementAndGet();
                return new Object();
            }

            @Override
            public void destroy(Object obj) {
                destroyed.incrementAndGet();
            }
        }, alwaysValid(), 2, 60_000L);
        Object a = pool.borrow(1_000L);
        Object b = pool.borrow(1_000L);
        pool.release(a); // a 回到空闲队列
        pool.close();
        check(destroyed.get() == 1, "idle object must be destroyed on close");
        expectIllegalState(new Runnable() {
            @Override
            public void run() {
                pool.borrow(0L);
            }
        });
        pool.release(b); // 关闭后归还：销毁而不是回池
        check(destroyed.get() == 2, "object released after close must be destroyed");
        check(pool.borrowedCount() == 0, "no outstanding borrows");
        pool.close(); // 幂等
        check(destroyed.get() == 2, "second close must be a no-op");
        passed++;
    }

    /** 校验器：失效对象在借出/归还时被销毁并替换。 */
    static void testValidatorEvictsBadObjects() {
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger destroyed = new AtomicInteger();
        final AtomicInteger state = new AtomicInteger(); // 偶数=有效
        ObjectPool<Object> pool = new ObjectPool<Object>(new ObjectFactory<Object>() {
            @Override
            public Object create() {
                created.incrementAndGet();
                return new Object();
            }

            @Override
            public void destroy(Object obj) {
                destroyed.incrementAndGet();
            }
        }, new Validator<Object>() {
            @Override
            public boolean isValid(Object obj) {
                return state.get() % 2 == 0;
            }
        }, 1, 60_000L);
        Object o = pool.borrow(1_000L);
        check(created.get() == 1, "one object created");
        state.incrementAndGet(); // 变为失效
        pool.release(o); // 归还时校验失败 -> 销毁
        check(destroyed.get() == 1, "invalid object must be destroyed on release");
        state.incrementAndGet(); // 恢复有效
        Object o2 = pool.borrow(1_000L);
        check(created.get() == 2, "a fresh object must be created");
        check(o2 != o, "must not hand out the destroyed object");
        pool.release(o2);
        pool.close();
        passed++;
    }

    // ------------------------------------------------------------------

    static ObjectPool<Object> newPool(int max, long leakThresholdMillis) {
        return new ObjectPool<Object>(new ObjectFactory<Object>() {
            @Override
            public Object create() {
                return new Object();
            }

            @Override
            public void destroy(Object obj) {
            }
        }, alwaysValid(), max, leakThresholdMillis);
    }

    static Validator<Object> alwaysValid() {
        return new Validator<Object>() {
            @Override
            public boolean isValid(Object obj) {
                return true;
            }
        };
    }

    static void expectIllegalState(Runnable r) {
        try {
            r.run();
        } catch (IllegalStateException expected) {
            return;
        }
        throw new AssertionError("expected IllegalStateException");
    }

    static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("CHECK FAILED: " + message);
        }
    }

    private SelfTest() {
    }
}
