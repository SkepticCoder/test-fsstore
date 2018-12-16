package ru.store.impl.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class ByteBufferWriteHandler implements CompletionHandler<Integer, ByteBuffer> {
    private final CompletableFuture<Boolean> handlerCons;
    private final Iterator<byte[]> data;
    private int position = 0;
    private AsynchronousFileChannel channel;

    public ByteBufferWriteHandler(AsynchronousFileChannel channel, CompletableFuture<Boolean> handlerCons, Iterator<byte[]> data) {
        this.handlerCons = handlerCons;
        this.data = data;
        this.channel = channel;
    }

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if(attachment.remaining() == 0) {
            if (!data.hasNext()) {
                close();
                handlerCons.complete(true);
                return;
            }
        } else {
            position += result;
            //pass the same completion handler
            channel.write(attachment, position, attachment, this);
            return;
        }

        position += result;
        byte[] value = data.next();
        ByteBuffer buffer = value == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(value);
        channel.write(buffer, position, buffer, this);
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        handlerCons.completeExceptionally(exc);
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
