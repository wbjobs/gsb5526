package com.gsb.pool;

/**
 * 对象校验器：判断一个池化对象当前是否仍然可用。
 */
public interface Validator<T> {

    /**
     * @return true 表示对象仍然有效，可以借出或归还到空闲队列；
     *         false 表示对象已失效，池会将其销毁。
     */
    boolean isValid(T obj);
}
