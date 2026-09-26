package com.gsb.pool;

/**
 * Validates objects before they are handed out by the pool.
 *
 * @param <T> the type of object managed by the pool
 */
public interface ObjectValidator<T> {

    /**
     * Returns {@code true} if the object is still usable.
     *
     * @param obj the object to validate
     * @return {@code true} if the object may be borrowed
     */
    boolean isValid(T obj);
}
