package ru.store.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueFactoryTest {

    @Test
    void create() throws Exception {
        KeyValueStore store = KeyValueFactory.create();

        assertNotNull(store);

        store.close();
    }

}