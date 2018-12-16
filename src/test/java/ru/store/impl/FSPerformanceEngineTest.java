package ru.store.impl;


import io.github.glytching.junit.extension.folder.TemporaryFolder;
import io.github.glytching.junit.extension.folder.TemporaryFolderExtension;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.store.api.KeyValueFactory;
import ru.store.api.KeyValueStore;
import ru.store.exceptions.StoreEngineOperationException;

import java.time.Instant;
import java.time.LocalTime;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(TemporaryFolderExtension.class)
public class FSPerformanceEngineTest {

    private KeyValueStore<String, byte[]> keyValueFactory;


    @BeforeEach
    void setUp(TemporaryFolder folder) throws StoreEngineOperationException {
        keyValueFactory = KeyValueFactory.create(folder.getRoot().getPath()).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        keyValueFactory.close();
    }


    @ParameterizedTest
    @MethodSource("keyValueProvider")
    void operateLongKeys(String key, byte[] value) throws InterruptedException, ExecutionException, TimeoutException {
        keyValueFactory.put(key, value, null).get(100, TimeUnit.SECONDS);

        assertArrayEquals(value, keyValueFactory.get(key));

        keyValueFactory.remove(key);
    }

    @Test
    void operateReadWriteMultiThread() {
        int countThread = 50000;
        CountDownLatch startSignal = new CountDownLatch(countThread);
        ExecutorService executorService = Executors.newFixedThreadPool(50,
                new BasicThreadFactory.Builder().namingPattern("read-threads-%d")
                        .build()
        );
        byte[] data = TestUtils.generateValue(1000000);
        IntStream.rangeClosed(1, countThread).forEach(i ->
        executorService.execute(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            long now = System.currentTimeMillis();
            future.handle((r, e) -> {
                startSignal.countDown();
                long duration = System.currentTimeMillis() - now;
                System.out.println(duration);
                return r;
            });
            keyValueFactory.put("key1", data, future);
        }));

        try {
            startSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    static Stream<Arguments> keyValueProvider() {
        return Stream.concat(
                Stream.of(arguments(null, TestUtils.generateValue(2))),
                IntStream.rangeClosed(0, 240).mapToObj(FSPerformanceEngineTest::keyValue));
    }

    static Arguments keyValue(int sizeKey) {
        return arguments(TestUtils.generateString(sizeKey), TestUtils.generateValue(2));

    }
}
