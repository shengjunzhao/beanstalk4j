package com.haole.mq.beanstalk.aio.channel;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * Created by shengjunzhao on 2017/8/12.
 */
public class AioContextInboud {

    private int blockSize = 512;
    private ByteBuffer buffer;
    private AsynchronousSocketChannel channel;
    private AioSocketChannelEventLoop eventLoop;

    public AioContextInboud(AsynchronousSocketChannel channel, AioSocketChannelEventLoop eventLoop) {
        this.channel = channel;
        this.eventLoop = eventLoop;
    }

    public ByteBuffer putByteBuffer(ByteBuffer byteBuffer) {
        if (null == buffer)
            buffer = ByteBuffer.allocateDirect(1024);
        byteBuffer.flip();
        int remainder = buffer.capacity() - buffer.position();
        if (byteBuffer.position() > remainder) {
            int capacity = buffer.position() + byteBuffer.position();
            int blockNum = capacity / blockSize;
            capacity = (blockNum + 1) * blockSize;
            ByteBuffer allocBuffer = ByteBuffer.allocateDirect(capacity);
            buffer.flip();
            allocBuffer.put(buffer);
            buffer = allocBuffer;
        }
        buffer.put(byteBuffer);
        return buffer;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public AsynchronousSocketChannel getChannel() {
        return channel;
    }

    public AioSocketChannelEventLoop getEventLoop() {
        return eventLoop;
    }
}
