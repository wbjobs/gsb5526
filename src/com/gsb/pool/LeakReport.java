package com.gsb.pool;

import java.util.Arrays;

/**
 * 一条泄漏报告：某个对象被借出后，超过池配置的泄漏判定时间仍未归还。
 * 报告在判定泄漏的那一刻生成，之后即使对象被归还，报告依然保留。
 */
public final class LeakReport {

    private final Object leakedObject;
    private final long borrowTimeMillis;
    private final long detectedTimeMillis;
    private final String borrowThreadName;
    private final StackTraceElement[] borrowStackTrace;

    LeakReport(Object leakedObject,
               long borrowTimeMillis,
               long detectedTimeMillis,
               String borrowThreadName,
               StackTraceElement[] borrowStackTrace) {
        this.leakedObject = leakedObject;
        this.borrowTimeMillis = borrowTimeMillis;
        this.detectedTimeMillis = detectedTimeMillis;
        this.borrowThreadName = borrowThreadName;
        this.borrowStackTrace = borrowStackTrace;
    }

    /** 被泄漏的对象本身。 */
    public Object getLeakedObject() {
        return leakedObject;
    }

    /** 借出时刻（System.currentTimeMillis 口径）。 */
    public long getBorrowTimeMillis() {
        return borrowTimeMillis;
    }

    /** 判定为泄漏的时刻（System.currentTimeMillis 口径）。 */
    public long getDetectedTimeMillis() {
        return detectedTimeMillis;
    }

    /** 借出该对象的线程名。 */
    public String getBorrowThreadName() {
        return borrowThreadName;
    }

    /** 借出时的调用堆栈（拷贝，调用方修改不影响池内部状态）。 */
    public StackTraceElement[] getBorrowStackTrace() {
        return Arrays.copyOf(borrowStackTrace, borrowStackTrace.length);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeakReport{object=").append(leakedObject);
        sb.append(", borrowTimeMillis=").append(borrowTimeMillis);
        sb.append(", detectedTimeMillis=").append(detectedTimeMillis);
        sb.append(", borrowThread=").append(borrowThreadName);
        sb.append(", borrowStackTrace=");
        for (int i = 0; i < borrowStackTrace.length; i++) {
            sb.append("\n\tat ").append(borrowStackTrace[i]);
        }
        sb.append('}');
        return sb.toString();
    }
}
