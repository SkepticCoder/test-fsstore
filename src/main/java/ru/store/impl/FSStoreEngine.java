package ru.store.impl;

import ru.store.exceptions.StoreEngineOperationException;
import ru.store.impl.async.ByteBufferReadHandler;
import ru.store.impl.async.ByteBufferWriteHandler;
import ru.store.impl.durability.Retrier;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FSStoreEngine implements StoreEngine<String, byte[]> {

    private static final Logger LOG = Logger.getLogger(FSStoreEngine.class.getName());

    private static final OpenOption[] OPEN_OPTIONS_ON_PUT = {StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption
            .TRUNCATE_EXISTING,
            StandardOpenOption.SYNC};

    private static final Set<OpenOption> OPEN_OPTIONS_ON_PUT_SET = new HashSet<>(Arrays.asList(OPEN_OPTIONS_ON_PUT));
    private static final OpenOption[] OPEN_OPTIONS_ON_GET = {StandardOpenOption.READ};
    private static final Set<OpenOption> OPEN_OPTIONS_ON_GET_SET = new HashSet<>(Arrays.asList(OPEN_OPTIONS_ON_GET));

    private static final CopyOption[] COPY_OPTIONS_ON_PUT = {StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING};
    private static final String DEFAULT_PREFIX_TEMP_DIR = ".temp";

    private static final int DEFAULT_COUNT_READ_THREAD = 2;
    private static final int DEFAULT_COUNT_WRITE_THREADS = 2;

    private static final int MAX_SIZE_BUFFER = Integer.MAX_VALUE - 8;
    private static final int SIZE_BYTE_BUFFER = 4096;
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int DEFAULT_COUNT_RETRY = 1000;
    public static final String DEFAULT_EXTENSION_DATA_FILE = ".data";


    private final Path tempPath;
    private final Path dataPath;
    private String extensionDataFile = DEFAULT_EXTENSION_DATA_FILE;

    private ExecutorService readerExecutor;

    private ExecutorService writerExecutor;
    private int countRetry = DEFAULT_COUNT_RETRY;

    private boolean trace = false;

    public FSStoreEngine(String dataPath) throws StoreEngineOperationException {
        this(dataPath, Paths.get(dataPath, DEFAULT_PREFIX_TEMP_DIR + System.currentTimeMillis()).toString(), DEFAULT_COUNT_READ_THREAD, DEFAULT_COUNT_WRITE_THREADS);
    }

    public FSStoreEngine(String dataPath, String tempPath, int countReadThreads, int countWriteThread) throws StoreEngineOperationException {
        this.dataPath = Paths.get(dataPath);
        this.tempPath = Paths.get(tempPath);

        try {
            Files.createDirectory(this.tempPath);
        } catch (IOException e) {
            throw new StoreEngineOperationException(String.format("Error create temp directory[%s]", tempPath), e);
        }
        readerExecutor = Executors.newFixedThreadPool(countReadThreads);
        writerExecutor = Executors.newFixedThreadPool(countWriteThread);
    }

    public String getExtensionDataFile() {
        return extensionDataFile;
    }

    public void setExtensionDataFile(String extensionDataFile) {
        this.extensionDataFile = extensionDataFile;
    }

    public void setCountRetry(int countRetry) {
        this.countRetry = countRetry;
    }

    public int getCountRetry() {
        return countRetry;
    }

    @Override
    public CompletableFuture<byte[]> read(String key) throws StoreEngineOperationException {
        if (key == null) {
            key = "null";
        }

        CompletableFuture<byte[]> result = new CompletableFuture<>();
        File file = getResultPath(key).toFile();
        if (!file.exists()) {
            result.complete(EMPTY_BYTES);
            return result;
        }

        validateSizeBuffer(file);
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) file.length());
        return read(key, null, byteBuffer::put).
                                                       thenApply(res -> res ? byteBuffer.array() : EMPTY_BYTES);
    }

    @Override
    public byte[] readSync(String key) throws StoreEngineOperationException {
        if (key == null) {
            key = "null";
        }

        Path path = getResultPath(key);
        if (!path.toFile().exists()) {
            return EMPTY_BYTES;
        }

        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new StoreEngineOperationException("Error read key " + key, e);
        }
    }

    @Override
    public CompletableFuture<Boolean> read(String key, Consumer<byte[]> listener) throws StoreEngineOperationException {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        File file = getResultPath(key).toFile();
        if (!file.exists()) {
            result.complete(true);
            listener.accept(EMPTY_BYTES);
            return result;
        }

        return read(key, null, listener);
    }

    private void validateSizeBuffer(File file) {
        if (file.length() > MAX_SIZE_BUFFER) {
            throw new IllegalArgumentException(String.format("Size file [%s Bytes] too big", file.length()));
        }
    }

    @Override
    public boolean remove(String key) throws StoreEngineOperationException {
        if (key == null) {
            key = "null";
        }

        Path sourcePath = getResultPath(key);
        Path tempPathKey = getTempPathByKey(key + "_d");
        if (!sourcePath.toFile().exists()) {
            return false;
        }
        move(key, sourcePath, tempPathKey);

        return true;
    }

    @Override
    public void close() throws StoreEngineOperationException {
        clear();

        try {
            Files.delete(tempPath);
        } catch (IOException e) {
            throw new StoreEngineOperationException("Error delete temp path", e);
        }
    }

    @Override
    public CompletableFuture<byte[]> write(String key, final byte[] value) throws StoreEngineOperationException {
        return write(key, Collections.singleton(value == null ? EMPTY_BYTES : value).iterator(), null)
                .thenApply(result -> result ? value : EMPTY_BYTES);
    }

    @Override
    public byte[] writeSync(String key, byte[] value) throws StoreEngineOperationException {
        if (key == null) {
            key = "null";
        }

        Path tempPathKey = getTempPathByKey(key);
        try {
            Files.write(tempPathKey, value, OPEN_OPTIONS_ON_PUT);
            moveWithDelete(tempPathKey, getResultPath(key));
        } catch (IOException e) {
            throw new StoreEngineOperationException("Error write on file", e);
        }

        return value;
    }

    @Override
    public CompletableFuture<Boolean> write(String key,
                                            Iterator<byte[]> data,
                                            CompletableFuture<Boolean> resultFuture) throws StoreEngineOperationException {
        final CompletableFuture<Boolean> handlerCons = resultFuture == null ? new CompletableFuture<>() : resultFuture;
        if (key == null) {
            key = "null";
        }

        Path tempPathKey = getTempPathByKey(key);
        byte[] value = data.next();
        ByteBuffer valueBuffer = value != null ? ByteBuffer.wrap(value) : ByteBuffer.allocate(0);
        final Path resultPathKey = getResultPath(key);

        resultFuture = handlerCons.thenApply(res -> {
            try {
                moveWithDelete(tempPathKey, resultPathKey);
            } catch (Exception e2) {
                handlerCons.completeExceptionally(e2);
            }
            return res;
        });

        AsynchronousFileChannel channel = null;
        try {
            channel = AsynchronousFileChannel.open(tempPathKey, OPEN_OPTIONS_ON_PUT_SET, writerExecutor);
            channel.write(valueBuffer, 0, valueBuffer, new ByteBufferWriteHandler(channel, handlerCons, data));
        } catch (IOException e) {
            FSUtils.close(channel);

            File file = tempPathKey.toFile();
            if (file.exists()) {
                try {
                    Files.deleteIfExists(tempPathKey);
                } catch (IOException e1) {
                    handlerCons.completeExceptionally(e1);
                }
            }
            handlerCons.completeExceptionally(e);
            throw new StoreEngineOperationException("Error close channel", e);
        }

        return resultFuture;
    }

    private void moveWithDelete(Path tempPathKey, Path resultPathKey) throws StoreEngineOperationException {
        try {
            new Retrier(countRetry, () -> Files.move(tempPathKey, resultPathKey, FSStoreEngine.COPY_OPTIONS_ON_PUT)).run();
        } catch (Throwable e) {
            try {
                Files.deleteIfExists(tempPathKey);
            } catch (IOException e1) {
                throw new StoreEngineOperationException("Error delete temp file ", e1);
            }

            throw new StoreEngineOperationException("Error move file ", e);
        }
    }

    private void move(String key, Path sourcePath, Path tempPathKey) throws StoreEngineOperationException {
        try {
            new Retrier(countRetry, () -> Files.move(sourcePath, tempPathKey, COPY_OPTIONS_ON_PUT)).run();
            new Retrier(countRetry, () -> Files.deleteIfExists(tempPathKey)).run(); // or file channel::trancate(0)
        } catch (Throwable e) {
            throw new StoreEngineOperationException("Error remove key " + key, e);
        }
    }


    @Override
    public int size() throws StoreEngineOperationException {
        return keys().size();
    }

    @Override
    public Set<String> keys() throws StoreEngineOperationException {
        Path dir = Paths.get(getDataPath());
        try (Stream<Path> streamPaths = Files.list(dir)) {
            return keyStream(streamPaths)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new StoreEngineOperationException("Error get keys ", e);
        }
    }

    private Stream<String> keyStream(Stream<Path> streamPaths) {
        return streamPaths.filter(path1 -> path1.toString().endsWith(extensionDataFile) && Files.isReadable(path1))
                          .map(Path::getFileName)
                          .map(Object::toString)
                          .map(FSUtils::removeExtension);
    }

    @Override
    public Collection<byte[]> values() throws StoreEngineOperationException {
        try (Stream<Path> streamPaths = Files.list(dataPath)) {
            return keyStream(streamPaths)
                    .map(key -> {
                        try {
                            return readSync(key);
                        } catch (StoreEngineOperationException e) {
                            LOG.warning(e.getMessage() + e.getCause().getMessage());
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new StoreEngineOperationException("Error get values", e);
        }
    }

    @Override
    public Stream<KeyValue<String, byte[]>> entryStream() throws StoreEngineOperationException {
        Path dir = Paths.get(getDataPath());
        try {
            return keyStream(Files.list(dir))
                    .map(key -> {
                        byte[] value;
                        try {
                            value = readSync(key);
                        } catch (StoreEngineOperationException e) {
                            LOG.warning(e.getMessage() + e.getCause().getMessage());
                            return null;
                        }
                        return new KeyValue<>(key, value);
                    }).filter(Objects::nonNull);

        } catch (IOException e) {
            throw new StoreEngineOperationException("Error entry stream", e);
        }
    }

    @Override
    public void clear() throws StoreEngineOperationException {
        if (!tempPath.toFile().exists()) {
            return;
        }

        try (Stream<Path> streamPaths = Files.list(dataPath)) {
            keyStream(streamPaths).forEach(key -> {
                try {
                    remove(key);
                } catch (StoreEngineOperationException e) {
                    LOG.warning(e.getMessage() + e.getCause().getMessage());
                }
            });
        } catch (IOException e) {
            throw new StoreEngineOperationException(String.format("Error clear store of dataPath[%s]", dataPath), e);
        }
    }

    String getDataPath() {
        return dataPath.toAbsolutePath().toString();
    }

    String getTempPathByKey() {
        return tempPath.toAbsolutePath().toString();
    }


    CompletableFuture<Boolean> read(String key,
                                    CompletableFuture<Boolean> finalFuture,
                                    Consumer<byte[]> listener) throws StoreEngineOperationException {
        final CompletableFuture<Boolean> finalFutureCons = finalFuture == null ? new CompletableFuture<>() : finalFuture;

        ByteBuffer valueBuffer = ByteBuffer.allocate(SIZE_BYTE_BUFFER);
        AsynchronousFileChannel channel = null;
        try {
            channel = AsynchronousFileChannel.open(getResultPath(key), OPEN_OPTIONS_ON_GET_SET, readerExecutor);
            channel.read(valueBuffer, 0, valueBuffer, new ByteBufferReadHandler(finalFutureCons, channel, listener));
        } catch (IOException e) {
            FSUtils.close(channel);

            LOG.fine(String.format("error write key %s : %s", key, e.getMessage()));
            finalFutureCons.completeExceptionally(e);
            throw new StoreEngineOperationException("Error read", e);
        }

        return finalFutureCons;
    }

    private Path getResultPath(String key) {
        return Paths.get(dataPath.resolve(key) + extensionDataFile);
    }

    private Path getTempPathByKey(String key) {
        return Paths.get(tempPath.resolve(getTempKey(key)) + extensionDataFile);
    }

    static String getTempKey(String key) {
        Thread currentThread = Thread.currentThread();
        return currentThread.getId() + key;
//
//        return new StringBuilder().append("tmp_")
//                                  .append(currentThread.getName())
//                                  .append('_')
//                                  .append(currentThread
//                                                  .getId())
//                                  .append('_')
//                                  .append(key)
//                                  .toString();
    }


}