package com.gsb.pool;

/**
 * An immutable report describing a single detected leak: an object that
 * stayed checked out longer than the pool's leak threshold.
 * <p>
 * A report is recorded at most once per checkout. Reports are kept by the
 * pool even if the object is eventually returned, so callers can inspect
 * historical leaks through {@link ObjectPool#leaks()}.
 */
public final class LeakReport {

    private final long borrowedAtMillis;
    private final long leakThresholdMillis;
    private final StackTraceElement[] borrowStackTrace;

    LeakReport(long borrowedAtMillis, long leakThresholdMillis, StackTraceElement[] borrowStackTrace) {
        this.borrowedAtMillis = borrowedAtMillis;
        this.leakThresholdMillis = leakThresholdMillis;
        this.borrowStackTrace = borrowStackTrace.clone();
    }

    /**
     * @return the time the object was borrowed, in milliseconds since the epoch
     */
    public long getBorrowedAtMillis() {
        return borrowedAtMillis;
    }

    /**
     * @return the leak threshold that was in effect, in milliseconds
     */
    public long getLeakThresholdMillis() {
        return leakThresholdMillis;
    }

    /**
     * @return a copy of the stack trace captured when the object was borrowed
     */
    public StackTraceElement[] getBorrowStackTrace() {
        return borrowStackTrace.clone();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeakReport{borrowedAtMillis=").append(borrowedAtMillis);
        sb.append(", leakThresholdMillis=").append(leakThresholdMillis);
        sb.append(", borrowStackTrace=");
        StackTraceElement[] trace = borrowStackTrace;
        for (int i = 0; i < trace.length; i++) {
            sb.append(i == 0 ? "[" : ", ").append(trace[i]);
        }
        sb.append("]}");
        return sb.toString();
    }
}
