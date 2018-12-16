package ru.store.api;

import ru.store.exceptions.StoreEngineOperationException;
import ru.store.impl.*;

import java.util.function.Function;

/**
 * Factory KeyValuesStore
 */
public final class KeyValueFactory {

    private KeyValueFactory() {
    }

    public static KeyValueBuilder<String, byte[]> create(String path) throws StoreEngineOperationException {
       return  new KeyValueBuilder<>(new KeyValueFSStoreWrapper(new FSStoreEngine(path, "temp1", 10, 10)));
    }

    public static KeyValueStore<String, byte[]> create() {
        try {
            return KeyValueFactory.create("").build();
        } catch (StoreEngineOperationException e) {
            return null;
        }
    }

    public static class KeyValueBuilder<K, V> {

        private KeyValueStore<K, V> keyValueStore;

        private KeyValueBuilder(KeyValueStore<K, V> keyValueStore) {
            this.keyValueStore = keyValueStore;
        }

        public <K1, V1> KeyValueBuilder<K1, V1> withKeyResolver(Function<K1, K> keyResolver, SerializerProvider<V, V1> valueSerializer) {
            return new KeyValueBuilder<>(new KeyValueStoreWithResolvers<>(keyValueStore, keyResolver, valueSerializer));
        }

        public KeyValueStore<K, V> build() {
            return keyValueStore;
        }
    }

}
