package com.gsb.pool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Self-contained verification for {@link ObjectPool}. No JUnit, no
 * third-party libraries: run {@code main} and it exits silently with
 * status 0 when every check passes, or throws on the first failure.
 *
 * <pre>
 * javac -d build $(find src -name '*.java')
 * java  -cp build com.gsb.pool.PoolSelfTest
 * </pre>
 */
public final class PoolSelfTest {

    private static int passed;

    public static void main(String[] args) throws Exception {
        testCapacityUnderConcurrency();
        testBorrowTimeout();
        testBorrowInterrupt();
        testForeignReleaseKeepsLedger();
        testDoubleReleaseKeepsLedger();
        testLeakReportedOnceWithStackAndTime();
        testValidatorEvictsStaleIdleObject();
        testClose();
        System.out.println("ALL " + passed + " CHECKS PASSED");
    }

    /** Hard requirement: checked-out count never exceeds maxCapacity,
     *  even with many threads racing and a slow factory. */
    private static void testCapacityUnderConcurrency() throws Exception {
        final int maxCapacity = 3;
        final ConnFactory factory = new ConnFactory(30); // slow create widens races
        final ObjectPool<Conn> pool =
                new ObjectPool<Conn>(factory, new ConnValidator(), maxCapacity, 60_000);
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final int threads = 12;
        final int iterations = 25;
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch go = new CountDownLatch(1);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(new Runnable() {
                @Override public void run() {
                    ready.countDown();
                    try {
                        go.await();
                        for (int n = 0; n < iterations; n++) {
                            Conn c = pool.borrow(10_000);
                            check(c != null, "borrow with generous timeout must succeed");
                            int now = inFlight.incrementAndGet();
                            peak.accumulateAndGet(now, Math::max);
                            Thread.sleep(2);
                            inFlight.decrementAndGet();
                            pool.release(c);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }
            });
            workers[i].start();
        }
        ready.await();
        go.countDown();
        for (Thread w : workers) {
            w.join();
        }
        check(failure.get() == null, "worker failed: " + failure.get());
        check(peak.get() > 0 && peak.get() <= maxCapacity,
                "peak in-flight " + peak.get() + " must be within capacity " + maxCapacity);
        check(factory.creates.get() <= maxCapacity,
                "factory created " + factory.creates.get() + " objects, capacity is " + maxCapacity);
        check(pool.getActiveCount() <= maxCapacity,
                "active count " + pool.getActiveCount() + " exceeds capacity");
        check(pool.leaks().isEmpty(), "no leaks expected when everything is released");
        pool.close();
        check(pool.getActiveCount() == 0, "all objects destroyed at close");
    }

    private static void testBorrowTimeout() {
        ConnFactory factory = new ConnFactory(0);
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 1, 60_000);
        Conn a = pool.borrow(1_000);
        check(a != null, "first borrow must succeed");
        long start = System.nanoTime();
        Conn b = pool.borrow(200);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        check(b == null, "borrow must return null on timeout");
        check(elapsedMillis >= 150, "borrow returned too early: " + elapsedMillis + "ms");
        pool.release(a);
        pool.close();
    }

    private static void testBorrowInterrupt() throws Exception {
        ConnFactory factory = new ConnFactory(0);
        final ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 1, 60_000);
        Conn a = pool.borrow(1_000);
        final AtomicReference<Conn> result = new AtomicReference<Conn>(a);
        Thread waiter = new Thread(new Runnable() {
            @Override public void run() {
                result.set(pool.borrow(10_000));
            }
        });
        waiter.start();
        Thread.sleep(150);
        waiter.interrupt();
        waiter.join(2_000);
        check(!waiter.isAlive(), "interrupted borrower must stop waiting");
        check(result.get() == null, "interrupted borrow must return null");
        pool.release(a);
        pool.close();
    }

    /** Hard requirement: releasing a foreign object throws and the
     *  capacity ledger does not move by a single permit. */
    private static void testForeignReleaseKeepsLedger() {
        ConnFactory factory = new ConnFactory(0);
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 1, 60_000);
        Conn a = pool.borrow(1_000);
        try {
            pool.release(new Conn(-1));
            throw new AssertionError("foreign release must throw IllegalStateException");
        } catch (IllegalStateException expected) {
            passed++;
        }
        try {
            pool.release(null);
            throw new AssertionError("null release must throw IllegalStateException");
        } catch (IllegalStateException expected) {
            passed++;
        }
        check(pool.borrow(150) == null, "ledger must be unchanged: still no permit available");
        check(pool.getActiveCount() == 1, "active count must be unchanged after foreign release");
        pool.release(a);
        check(pool.borrow(150) != null, "permit must come back exactly once");
        pool.close();
    }

    /** Hard requirement: releasing the same object twice throws and the
     *  capacity ledger does not move by a single permit. */
    private static void testDoubleReleaseKeepsLedger() {
        ConnFactory factory = new ConnFactory(0);
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 1, 60_000);
        Conn a = pool.borrow(1_000);
        pool.release(a);
        try {
            pool.release(a);
            throw new AssertionError("double release must throw IllegalStateException");
        } catch (IllegalStateException expected) {
            passed++;
        }
        Conn b = pool.borrow(150);
        check(b != null, "the single real permit must still be usable");
        check(pool.borrow(150) == null, "double release must not create an extra permit");
        check(pool.getActiveCount() == 1, "active count must be unchanged after double release");
        pool.release(b);
        pool.close();
    }

    /** Hard requirement: a checkout older than the leak threshold is
     *  recorded once, with borrow time and borrow stack trace. */
    private static void testLeakReportedOnceWithStackAndTime() throws Exception {
        ConnFactory factory = new ConnFactory(0);
        long threshold = 300;
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 2, threshold);

        Conn a = pool.borrow(1_000);
        long borrowedAt = System.currentTimeMillis();
        Thread.sleep(threshold + 300);
        List<LeakReport> leaks = pool.leaks();
        check(leaks.size() == 1, "exactly one leak expected, got " + leaks.size());
        LeakReport report = leaks.get(0);
        check(Math.abs(report.getBorrowedAtMillis() - borrowedAt) < 300,
                "report must carry the borrow time, got " + report.getBorrowedAtMillis());
        check(report.getBorrowStackTrace().length > 0, "report must carry the borrow stack trace");
        check(report.getLeakThresholdMillis() == threshold, "report must carry the threshold");

        check(pool.leaks().size() == 1, "same checkout must not be reported twice");
        pool.release(a);
        check(pool.leaks().size() == 1, "report must be kept after the object is returned");

        Conn b = pool.borrow(1_000);
        pool.release(b);
        check(pool.leaks().size() == 1, "promptly returned object must not be reported");

        Conn c = pool.borrow(1_000);
        Thread.sleep(threshold + 300);
        check(pool.leaks().size() == 2, "a second leaked checkout must add a second report");
        pool.release(c);
        pool.close();
    }

    private static void testValidatorEvictsStaleIdleObject() {
        ConnFactory factory = new ConnFactory(0);
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 1, 60_000);
        Conn a = pool.borrow(1_000);
        pool.release(a);
        a.valid = false; // simulate a connection that died while idle
        Conn b = pool.borrow(1_000);
        check(b != null && b != a, "stale idle object must be evicted and replaced");
        check(factory.destroys.get() == 1, "stale object must be destroyed");
        pool.release(b);
        pool.close();
    }

    private static void testClose() {
        ConnFactory factory = new ConnFactory(0);
        ObjectPool<Conn> pool = new ObjectPool<Conn>(factory, new ConnValidator(), 2, 60_000);
        Conn a = pool.borrow(1_000);
        Conn b = pool.borrow(1_000);
        pool.release(b); // b goes back to idle
        pool.close();
        check(factory.destroys.get() == 1, "idle object must be destroyed at close");
        try {
            pool.borrow(100);
            throw new AssertionError("borrow after close must throw IllegalStateException");
        } catch (IllegalStateException expected) {
            passed++;
        }
        pool.release(a); // still checked out: destroyed on release
        check(factory.destroys.get() == 2, "checked-out object must be destroyed on release after close");
        check(pool.getActiveCount() == 0, "no live objects after close");
        pool.close(); // idempotent
        check(factory.destroys.get() == 2, "second close must be a no-op");
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError("CHECK FAILED: " + what);
        }
        passed++;
    }

    /** Stand-in for a pooled resource such as a connection. */
    static final class Conn {
        final int id;
        volatile boolean valid = true;

        Conn(int id) {
            this.id = id;
        }
    }

    static final class ConnFactory implements ObjectFactory<Conn> {
        final AtomicInteger creates = new AtomicInteger();
        final AtomicInteger destroys = new AtomicInteger();
        private final long createDelayMillis;

        ConnFactory(long createDelayMillis) {
            this.createDelayMillis = createDelayMillis;
        }

        @Override public Conn create() throws Exception {
            if (createDelayMillis > 0) {
                Thread.sleep(createDelayMillis);
            }
            return new Conn(creates.incrementAndGet());
        }

        @Override public void destroy(Conn obj) {
            destroys.incrementAndGet();
        }
    }

    static final class ConnValidator implements ObjectValidator<Conn> {
        @Override public boolean isValid(Conn obj) {
            return obj.valid;
        }
    }
}
