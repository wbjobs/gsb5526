# gsb-pool

带泄漏检测的通用对象池，纯 JDK 8 标准库实现，无任何第三方依赖。

## 编译与自测

```sh
javac -source 8 -target 8 -d build $(find src -name '*.java')
java  -cp build com.gsb.pool.PoolSelfTest
```

`PoolSelfTest` 不依赖 JUnit，全部检查通过时输出 `ALL ... CHECKS PASSED`，
任一检查失败立即抛 `AssertionError` 并以非零状态退出。

## API 一览

```java
ObjectPool<T> pool = new ObjectPool<T>(factory, validator, maxCapacity, leakThresholdMillis);
T obj = pool.borrow(timeoutMillis);   // 超时或中断返回 null；池已关闭抛 IllegalStateException
pool.release(obj);                    // 归还；外来对象/重复归还抛 IllegalStateException，账目不变
List<LeakReport> leaks = pool.leaks(); // 超过泄漏阈值仍未归还的记录（含借出时间与借出堆栈）
pool.close();                         // 销毁空闲对象；未归还的对象在 release 时销毁
```

- `ObjectFactory<T>`：`create()` / `destroy(T)`，负责对象的生命周期。
- `ObjectValidator<T>`：`isValid(T)`，借出前校验，失效的空闲对象会被销毁并重建。
- 容量账目：任何时刻借出对象数不超过 `maxCapacity`；已获准但仍在创建中的
  对象同样占用额度，等待中的线程不会放大容量。
- 泄漏检测：每次借出记录借出时间与借方堆栈，超过 `leakThresholdMillis`
  未归还即在 `leaks()` 中记录一次，归还后报告仍保留。
