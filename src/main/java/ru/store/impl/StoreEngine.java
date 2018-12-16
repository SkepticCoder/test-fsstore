package ru.store.impl;

import ru.store.exceptions.StoreEngineOperationException;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface StoreEngine<K extends Comparable, V> extends AutoCloseable {

    CompletableFuture<V> write(K key, V value) throws StoreEngineOperationException;

    V writeSync(String key, V value) throws StoreEngineOperationException;

    CompletableFuture<V> read(String key) throws StoreEngineOperationException;

    V readSync(String key) throws StoreEngineOperationException;

    CompletableFuture<Boolean> read(String key, Consumer<V> listener) throws StoreEngineOperationException;

    CompletableFuture<Boolean> write(String key, Iterator<V> dataIter, CompletableFuture<Boolean> completableFuture) throws StoreEngineOperationException;

    boolean remove(K key) throws StoreEngineOperationException;

    int size() throws StoreEngineOperationException;

    Set<K> keys() throws StoreEngineOperationException;

    Collection<V> values() throws StoreEngineOperationException;

    Stream<KeyValue<K, V>> entryStream() throws StoreEngineOperationException;

    void clear() throws StoreEngineOperationException;
}
