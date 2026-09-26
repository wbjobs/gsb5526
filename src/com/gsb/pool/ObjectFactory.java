package com.gsb.pool;

/**
 * 对象工厂：负责创建和销毁池化对象。
 */
public interface ObjectFactory<T> {

    /**
     * 创建一个新对象。抛出的 RuntimeException 会原样传播给 borrow 调用方。
     */
    T create();

    /**
     * 销毁一个对象（释放其占用的资源）。实现应当尽量容忍重复/异常调用，
     * 池内部调用此方法时会捕获并忽略所有异常。
     */
    void destroy(T obj);
}
