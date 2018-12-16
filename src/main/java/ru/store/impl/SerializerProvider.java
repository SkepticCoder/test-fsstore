package ru.store.impl;

import java.util.function.Function;

public final class SerializerProvider<V, V1> {

    private final Function<V, V1> serializer;

    private final Function<V1, V> deSerializer;

    public SerializerProvider(Function<V, V1> serializer, Function<V1, V> deSerializer) {
        this.serializer = serializer;
        this.deSerializer = deSerializer;
    }

    public V1 serialize(V value) {
        return serializer.apply(value);
    }

    public V deSerialize(V1 value) {
        return deSerializer.apply(value);
    }

    public Function<V, V1> getSerializer() {
        return serializer;
    }

    public Function<V1, V> getDeSerializer() {
        return deSerializer;
    }
}
