package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.aio.Callback;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public class BeanstalkBufferWriter implements Callback<WriteCallback> {

    private AsynchronousSocketChannel channel;
    private ByteBuffer buffer;

    public BeanstalkBufferWriter(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    public void write(ByteBuffer buffer, WriteCallback write) {
        buffer.flip();
        this.buffer = buffer;
        channel.write(buffer, write, this);
    }

    @Override
    public void completed(Integer result, WriteCallback context) {
        if (buffer.hasRemaining())
            channel.write(buffer, context, this);
        else {
            buffer = null;
            context.writeCompleted();
        }

    }

    @Override
    public void failed(Throwable cause, WriteCallback context) {
        buffer = null;
        context.writeFailed(cause);
    }

    public void close() throws IOException {
        if (channel.isOpen())
            channel.close();
    }
}
