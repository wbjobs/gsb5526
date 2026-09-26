package com.gsb.pool;

/**
 * Creates and destroys pooled objects.
 *
 * @param <T> the type of object managed by the pool
 */
public interface ObjectFactory<T> {

    /**
     * Creates a new object. Implementations may throw any exception;
     * the pool wraps checked exceptions in a {@link RuntimeException}.
     *
     * @return a new object, never {@code null}
     * @throws Exception if creation fails
     */
    T create() throws Exception;

    /**
     * Destroys an object that is no longer needed by the pool.
     *
     * @param obj the object to destroy
     * @throws Exception if destruction fails
     */
    void destroy(T obj) throws Exception;
}
