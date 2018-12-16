package ru.store.impl.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ByteBufferReadHandler implements CompletionHandler<Integer, ByteBuffer> {

    private final CompletableFuture<Boolean> handlerCons;
    private final AsynchronousFileChannel channel;
    private final Consumer<byte[]> consumer;
    private int position = 0;

    public ByteBufferReadHandler(CompletableFuture<Boolean> handlerCons, AsynchronousFileChannel channel,
                                 Consumer<byte[]> consumer) {
        this.handlerCons = handlerCons;
        this.channel = channel;
        this.consumer = consumer;
    }

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if(result != -1) {
            try {
                attachment.flip();
                byte[] res = new byte[result];
                attachment.get(res);
                consumer.accept(res);
            } catch (Exception e) {
                handlerCons.completeExceptionally(e);
                return;
            }

            attachment.clear();
            position += result;
            //pass the same completion handler
            channel.read(attachment, position, attachment, this);
        } else {
            close();
            handlerCons.complete(true);
        }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        if (exc instanceof NoSuchFileException) {
            handlerCons.complete(null);
        } else {
            handlerCons.completeExceptionally(exc);
    }
        close();
    }

    private void close() {
        if (channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
