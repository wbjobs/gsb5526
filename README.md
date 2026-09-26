# gsb-pool

带泄漏检测的通用对象池。JDK 8，仅使用标准库，无任何第三方依赖。

## 编译与自测

```sh
javac -encoding UTF-8 -d out $(find src -name '*.java')
java -cp out com.gsb.pool.SelfTest
```

自测覆盖：并发容量不变式、超时返回 null、重复/外来 release 抛
IllegalStateException 且账目不变、泄漏报告（含借出堆栈与借出时间）、
close 行为、校验器淘汰失效对象。

## API

- `new ObjectPool<T>(factory, validator, maxCapacity, leakThresholdMillis)`
- `T borrow(long timeoutMillis)` — 超时返回 `null`，池关闭抛 `IllegalStateException`
- `void release(T obj)` — 外来对象或重复归还抛 `IllegalStateException`，账目不变
- `List<LeakReport> leaks()` — 泄漏报告快照（借出堆栈、借出时间、借出线程）
- `void close()` — 幂等；空闲对象立即销毁，在借对象归还时销毁

## 容量账目模型

内部使用许可数为 `maxCapacity` 的公平信号量：先拿许可，再复用/创建对象，
对象持有期间许可不归还。因此不变式为 **已借出数 + 借出在途数 <= maxCapacity**；
等待中的线程排在信号量队列里，不会导致超发。许可只在 `release` 成功路径归还。
