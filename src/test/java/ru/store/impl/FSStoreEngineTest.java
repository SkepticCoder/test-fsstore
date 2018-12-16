package ru.store.impl;

import io.github.glytching.junit.extension.folder.TemporaryFolder;
import io.github.glytching.junit.extension.folder.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.store.exceptions.StoreEngineOperationException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TemporaryFolderExtension.class)
class FSStoreEngineTest {

    private FSStoreEngine fsStoreEngine;


    @BeforeEach
    void setUp(TemporaryFolder folder) throws StoreEngineOperationException {
        fsStoreEngine = new FSStoreEngine(folder.getRoot().getPath(), Paths.get(folder.getRoot().getPath(),"tmp").toString(), 1, 1);
    }

    @AfterEach
    void tearDown() throws StoreEngineOperationException {

    }

    @Test
    void writeAsync() throws ExecutionException, InterruptedException, TimeoutException, StoreEngineOperationException {
        byte[] expectedRaw = TestUtils.generateValue(2000);

        byte[] result = fsStoreEngine.write("key", expectedRaw)
                                     .thenApply(r -> {
                                         try {
                                             return fsStoreEngine.readSync("key");
                                         } catch (StoreEngineOperationException e) {
                                             e.printStackTrace();
                                         }
                                         return null;
                                     }).get(10, TimeUnit.SECONDS);

        assertArrayEquals(expectedRaw, result);
    }

    @Test
    void size() throws InterruptedException, ExecutionException, TimeoutException, StoreEngineOperationException {
        int expectedSize = 5;
        for (int i = 0; i < expectedSize; i++) {
            fsStoreEngine.write("key" + i, "value".getBytes(StandardCharsets.UTF_8)).get(50, TimeUnit.SECONDS);
        }

        assertEquals(expectedSize, fsStoreEngine.size());
    }

    @Test
    void keys() throws InterruptedException, ExecutionException, TimeoutException, StoreEngineOperationException {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            keys.add("key" + i);
            fsStoreEngine.write("key" + i, "value".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);
        }

        assertEquals(keys, fsStoreEngine.keys());
    }

    @Test
    void values() throws InterruptedException, ExecutionException, TimeoutException, StoreEngineOperationException {
        byte[] expectedRaw = TestUtils.generateValue(1000, 2000);

        fsStoreEngine.write("key1", expectedRaw).get(5, TimeUnit.SECONDS);
        fsStoreEngine.write("key2", expectedRaw).get(5, TimeUnit.SECONDS);
        Collection<byte[]> values = fsStoreEngine.values();

        assertEquals(2, values.size());
        for (byte[] value : values) {
            assertArrayEquals(expectedRaw, value);
        }
    }

    @Test
    void removeDoestExist() throws ExecutionException, InterruptedException, TimeoutException, StoreEngineOperationException {
        assertFalse(fsStoreEngine.remove("key"));

        fsStoreEngine.read("value")
                     .thenAccept(result -> Assertions.assertArrayEquals(result, new byte[0]))
                     .get(5, TimeUnit.SECONDS);
    }

    @Test
    void removeExist() throws ExecutionException, InterruptedException, TimeoutException, StoreEngineOperationException {
        byte[] expectedRaw = "value".getBytes(StandardCharsets.UTF_8);
        fsStoreEngine.write("key", expectedRaw).get(5, TimeUnit.SECONDS);

        assertTrue(fsStoreEngine.remove("key"));

        fsStoreEngine.read("key")
                     .thenAccept(result -> Assertions.assertArrayEquals(result, new byte[0]))
                     .get(5, TimeUnit.SECONDS);
    }

    @Test
    void removeExistNullValue() throws ExecutionException, InterruptedException, TimeoutException, StoreEngineOperationException {
        fsStoreEngine.write("key", null).thenAccept(s -> {
            try {
                assertTrue(fsStoreEngine.remove("key"));
            } catch (StoreEngineOperationException e) {
                e.printStackTrace();
            }
        }).get(5, TimeUnit.SECONDS);


        fsStoreEngine.read("key")
                     .thenAccept(result -> Assertions.assertArrayEquals(result, new byte[0]))
                     .get(5, TimeUnit.SECONDS);
    }

    @Test
    void writeReadIterable() throws ExecutionException, InterruptedException, TimeoutException, StoreEngineOperationException {
        Collection<byte[]> values = TestUtils.generateValues(10000, 10_000, 10_000).collect(Collectors.toList());
        int size = values.stream().map(v -> v.length).reduce(0, Integer::sum);
        ByteBuffer expectedBytes = ByteBuffer.allocate(size);
        values.forEach(expectedBytes::put);

        assertTrue(fsStoreEngine.write("key", values.iterator(), null).get(5, TimeUnit.SECONDS));

        ByteBuffer resultBytes = ByteBuffer.allocate(size);
        fsStoreEngine.read("key", resultBytes::put).get(5, TimeUnit.SECONDS);


        assertArrayEquals(expectedBytes.array(), resultBytes.array());

    }

}