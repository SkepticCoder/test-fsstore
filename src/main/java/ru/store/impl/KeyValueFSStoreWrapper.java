package ru.store.impl;

import ru.store.api.KeyValueStore;
import ru.store.exceptions.StoreEngineOperationException;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


public final class KeyValueFSStoreWrapper implements KeyValueStore<String, byte[]> {

    private final StoreEngine<String, byte[]> storeEngine;

    public KeyValueFSStoreWrapper(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }


    @Override
    public CompletableFuture<Boolean> put(String key, byte[] value, CompletableFuture<Boolean> handler) {
        try {
            return storeEngine.write(key, Collections.singleton(value).iterator(), handler);
        } catch (StoreEngineOperationException e) {
            handler.completeExceptionally(e);
            return handler;
        }
    }

    @Override
    public boolean put(String key, byte[] value) {
        try {
            storeEngine.writeSync(key, value);
            return true;
        } catch (StoreEngineOperationException e) {
            return false;
        }
    }

    @Override
    public CompletableFuture<byte[]> get(String key, CompletableFuture<byte[]> handler) {
        return null;
    }

    @Override
    public byte[] get(String key) {
        try {
            return storeEngine.readSync(key);
        } catch (StoreEngineOperationException e) {
            return null;
        }
    }

    @Override
    public void remove(String key) {
        try {
            storeEngine.remove(key);
        } catch (StoreEngineOperationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Stream<KeyValue<String, byte[]>> entries() {
        try {
            return storeEngine.entryStream();
        } catch (StoreEngineOperationException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Set<String> keys() {
        try {
            return storeEngine.keys();
        } catch (StoreEngineOperationException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Collection<byte[]> values() {
        try {
            return storeEngine.values();
        } catch (StoreEngineOperationException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void clear() {
        try {
            storeEngine.clear();
        } catch (StoreEngineOperationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        storeEngine.close();
    }
}
