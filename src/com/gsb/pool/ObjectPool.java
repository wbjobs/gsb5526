package com.gsb.pool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe object pool with capacity accounting and leak detection.
 * <p>
 * Capacity accounting: the pool hands out at most {@code maxCapacity}
 * objects at any moment. A fair semaphore with {@code maxCapacity} permits
 * is the ledger: every borrower must hold a permit while it waits for,
 * creates, validates or uses an object, so the number of checked-out
 * objects plus the number of threads waiting inside the pool never
 * exceeds {@code maxCapacity}. Further borrowers block (up to their
 * timeout) until a permit is released.
 * <p>
 * Leak detection: each checkout records the borrow time and the borrowing
 * thread's stack trace. {@link #leaks()} reports every checkout that has
 * been outstanding for at least the leak threshold, at most once per
 * checkout. Reports are kept even after the object is returned.
 *
 * @param <T> the type of pooled object
 */
public class ObjectPool<T> implements AutoCloseable {

    private final ObjectFactory<T> factory;
    private final ObjectValidator<T> validator;
    private final int maxCapacity;
    private final long leakThresholdMillis;

    /** Capacity ledger: one permit per object that may be in flight. */
    private final Semaphore admission;

    private final ReentrantLock lock = new ReentrantLock();
    private final Deque<T> idle = new ArrayDeque<T>();
    private final IdentityHashMap<T, Lease> checkedOut = new IdentityHashMap<T, Lease>();
    private final List<LeakReport> leakReports = new ArrayList<LeakReport>();

    /** Number of live objects: idle + checked out + currently being created. */
    private int active;
    private boolean closed;

    /**
     * Creates a pool.
     *
     * @param factory             creates and destroys objects
     * @param validator           validates objects before they are handed out
     * @param maxCapacity         maximum number of objects in flight at any time
     * @param leakThresholdMillis how long a checkout may last before it is
     *                            reported as a leak
     */
    public ObjectPool(ObjectFactory<T> factory, ObjectValidator<T> validator,
                      int maxCapacity, long leakThresholdMillis) {
        if (factory == null) {
            throw new NullPointerException("factory");
        }
        if (validator == null) {
            throw new NullPointerException("validator");
        }
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be positive: " + maxCapacity);
        }
        if (leakThresholdMillis <= 0) {
            throw new IllegalArgumentException("leakThresholdMillis must be positive: " + leakThresholdMillis);
        }
        this.factory = factory;
        this.validator = validator;
        this.maxCapacity = maxCapacity;
        this.leakThresholdMillis = leakThresholdMillis;
        this.admission = new Semaphore(maxCapacity, true);
    }

    /**
     * Borrows an object, waiting up to {@code timeoutMillis} for capacity
     * to become available.
     *
     * @param timeoutMillis maximum time to wait; {@code 0} means no waiting
     * @return a pooled object, or {@code null} if the wait timed out or the
     *         thread was interrupted while waiting
     * @throws IllegalStateException if the pool is closed, if the factory
     *         fails to create a usable object, or if validation of a newly
     *         created object fails
     */
    public T borrow(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be >= 0: " + timeoutMillis);
        }
        boolean acquired;
        try {
            acquired = admission.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        if (!acquired) {
            return null;
        }
        boolean success = false;
        try {
            T obj = obtain();
            registerCheckout(obj);
            success = true;
            return obj;
        } finally {
            if (!success) {
                admission.release();
            }
        }
    }

    /**
     * Returns an object to the pool.
     *
     * @param obj an object previously borrowed from this pool
     * @throws IllegalStateException if {@code obj} is not currently checked
     *         out from this pool (foreign object, {@code null}, or already
     *         released); the capacity ledger is left untouched in that case
     */
    public void release(T obj) {
        boolean destroy;
        lock.lock();
        try {
            Lease lease = checkedOut.remove(obj);
            if (lease == null) {
                throw new IllegalStateException(
                        "object is not checked out from this pool (foreign, null or already released)");
            }
            destroy = closed;
            if (closed) {
                active--;
            } else {
                idle.addLast(obj);
            }
        } finally {
            lock.unlock();
        }
        admission.release();
        if (destroy) {
            destroyQuietly(obj);
        }
    }

    /**
     * Closes the pool. Idle objects are destroyed, waiting and future
     * borrowers fail with {@link IllegalStateException}, and objects that
     * are still checked out are destroyed when they are released.
     * Closing more than once has no effect.
     */
    @Override
    public void close() {
        List<T> toDestroy;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            toDestroy = new ArrayList<T>(idle);
            idle.clear();
            active -= toDestroy.size();
        } finally {
            lock.unlock();
        }
        // Wake threads blocked on the admission semaphore; they will see the
        // closed flag and fail fast. Permits no longer matter once closed.
        admission.release(maxCapacity);
        for (T obj : toDestroy) {
            destroyQuietly(obj);
        }
    }

    /**
     * Records leaks for every checkout that has been outstanding for at
     * least the leak threshold and returns a snapshot of all reports
     * recorded so far, including older ones.
     *
     * @return a snapshot of the recorded leak reports, never {@code null}
     */
    public List<LeakReport> leaks() {
        long now = System.currentTimeMillis();
        lock.lock();
        try {
            for (Map.Entry<T, Lease> entry : checkedOut.entrySet()) {
                Lease lease = entry.getValue();
                if (!lease.leakRecorded && now - lease.borrowedAtMillis >= leakThresholdMillis) {
                    lease.leakRecorded = true;
                    leakReports.add(new LeakReport(lease.borrowedAtMillis, leakThresholdMillis,
                            lease.borrowStackTrace));
                }
            }
            return new ArrayList<LeakReport>(leakReports);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return the maximum number of objects this pool may have in flight
     */
    public int getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * @return the leak threshold in milliseconds
     */
    public long getLeakThresholdMillis() {
        return leakThresholdMillis;
    }

    /**
     * @return the number of live objects: idle plus checked out plus
     *         currently being created
     */
    public int getActiveCount() {
        lock.lock();
        try {
            return active;
        } finally {
            lock.unlock();
        }
    }

    private T obtain() {
        for (;;) {
            T obj;
            boolean create;
            lock.lock();
            try {
                if (closed) {
                    throw new IllegalStateException("pool is closed");
                }
                obj = idle.pollFirst();
                create = obj == null;
                if (create) {
                    active++;
                }
            } finally {
                lock.unlock();
            }

            if (create) {
                try {
                    obj = factory.create();
                } catch (RuntimeException e) {
                    decrementActive();
                    throw e;
                } catch (Exception e) {
                    decrementActive();
                    throw new IllegalStateException("factory failed to create an object", e);
                }
                if (obj == null) {
                    decrementActive();
                    throw new IllegalStateException("factory returned null");
                }
                boolean valid;
                try {
                    valid = isValid(obj);
                } catch (RuntimeException e) {
                    destroyQuietly(obj);
                    decrementActive();
                    throw e;
                }
                if (!valid) {
                    destroyQuietly(obj);
                    decrementActive();
                    throw new IllegalStateException("newly created object failed validation");
                }
                return obj;
            }

            boolean valid;
            try {
                valid = isValid(obj);
            } catch (RuntimeException e) {
                destroyQuietly(obj);
                decrementActive();
                throw e;
            }
            if (valid) {
                return obj;
            }
            // Stale idle object: destroy it and retry with another idle
            // object or a freshly created one.
            destroyQuietly(obj);
            decrementActive();
        }
    }

    private boolean isValid(T obj) {
        try {
            return validator.isValid(obj);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("validator failed", e);
        }
    }

    private void registerCheckout(T obj) {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        long now = System.currentTimeMillis();
        boolean destroy;
        lock.lock();
        try {
            if (closed) {
                // close() raced with this checkout: dispose of the object
                // and keep the ledger consistent instead of handing it out.
                active--;
                destroy = true;
            } else {
                checkedOut.put(obj, new Lease(now, trace));
                destroy = false;
            }
        } finally {
            lock.unlock();
        }
        if (destroy) {
            destroyQuietly(obj);
            throw new IllegalStateException("pool is closed");
        }
    }

    private void decrementActive() {
        lock.lock();
        try {
            active--;
        } finally {
            lock.unlock();
        }
    }

    private void destroyQuietly(T obj) {
        try {
            factory.destroy(obj);
        } catch (Exception ignored) {
            // Destruction failures must not corrupt the pool's accounting.
        }
    }

    private static final class Lease {
        final long borrowedAtMillis;
        final StackTraceElement[] borrowStackTrace;
        boolean leakRecorded;

        Lease(long borrowedAtMillis, StackTraceElement[] borrowStackTrace) {
            this.borrowedAtMillis = borrowedAtMillis;
            this.borrowStackTrace = borrowStackTrace;
        }
    }
}
