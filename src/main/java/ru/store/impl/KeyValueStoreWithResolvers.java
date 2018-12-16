package ru.store.impl;

import ru.store.api.KeyValueStore;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyValueStoreWithResolvers<K1, V1, K2, V2> implements KeyValueStore<K1, V1> {

    private final KeyValueStore<K2, V2> store;

    private final Function<K1, K2> resolverKey;

    private final SerializerProvider<V1, V2> serializer;


    public KeyValueStoreWithResolvers(KeyValueStore<K2, V2> store, Function<K1, K2> resolverKey, SerializerProvider serializer) {
        this.store = store;
        this.serializer = serializer;
        this.resolverKey = resolverKey;
    }

    @Override
    public CompletableFuture<Boolean> put(K1 key, V1 value, CompletableFuture<Boolean> handler) {
        return store.put(resolveKey(key), serializer.serialize(value), handler);
    }

    @Override
    public boolean put(K1 key, V1 value) {
        return store.put(resolveKey(key), serializer.serialize(value));
    }

    @Override
    public CompletableFuture<V1> get(K1 key, CompletableFuture<V1> handler) {
        return store.get(resolveKey(key), handler.thenApply(serializer.getSerializer()))
                    .thenApply(serializer.getDeSerializer());
    }

    @Override
    public V1 get(K1 key) {
        return serializer.deSerialize(store.get(resolveKey(key)));
    }

    @Override
    public void remove(K1 key) {
        store.remove(resolveKey(key));
    }

    @Override
    public Stream<KeyValue<K1, V1>> entries() {
        return null;
    }

    @Override
    public Set<K1> keys() {
        return null;
    }

    @Override
    public Set<V1> values() {
        return store.values().stream()
                    .map(serializer.getDeSerializer())
                    .collect(Collectors.toSet());
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void close() throws Exception {
        store.close();
    }

    private K2 resolveKey(K1 key) {
        return resolverKey.apply(key);
    }
}
