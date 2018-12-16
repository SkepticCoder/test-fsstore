package ru.store.api;


import ru.store.impl.KeyValue;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * An store that maps keys to values.
 * The {@code KeyValueStore} describe simplify interface Dictionary Store which allow
 * <B> thread-safe</B> base operations {put, get, delete} and iteration over the keys and values
 * The values store has binary format (array of bytes, <i>byte[]</i>)
 *
 * @author Dmitrii Shakshin <d.shakshin@gmail.com>
 */
public interface KeyValueStore<K, V> extends AutoCloseable {

    /**
     * Associates the specified value with the specified key <br>
     * async operation
     * @param key
     * @param value
     * @param handler
     * @return handler
     */
    CompletableFuture<Boolean> put(K key, V value, CompletableFuture<Boolean> handler);

    /**
     * Associates the specified value with the specified key <br>
     * sync operation
     * @param key
     * @param value
     */
    boolean put(K key, V value);

    CompletableFuture<V> get(K key, CompletableFuture<V> handler);

    /**
     * Returns the value to which the specified key is mapped
     * @param the key whose associated value is to be returned
     * @return the value to which the specified key is mappe
     * or {@code null} if this map contains no mapping
     */
    V get(K key);

    void remove(K key);

    Stream<KeyValue<K, V>> entries();

    Set<K> keys();

    Collection<V> values();

    void clear();

}
