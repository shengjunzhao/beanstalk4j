package com.haole.mq.beanstalk.aio.handler;


import com.haole.mq.beanstalk.aio.channel.AioContextInboud;
import com.haole.mq.beanstalk.aio.channel.AioSocketChannelEventLoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;

/**
 * Created by shengjunzhao on 2017/8/12.
 */
public class AioSocketChannelReader extends AbstractReadCallback<ResponseCallback<AioContextInboud>> {

    private ByteBuffer buffer;
    private AsynchronousSocketChannel channel;
    private AioContextInboud contextInboud;

    public AioSocketChannelReader(AsynchronousSocketChannel channel, AioSocketChannelEventLoop eventLoop) {
        this.channel = channel;
        contextInboud = new AioContextInboud(channel, eventLoop);
    }

    public void read(ResponseCallback<AioContextInboud> protocol) {
        if (buffer == null)
            buffer = ByteBuffer.allocateDirect(1024);
        buffer.clear();
        channel.read(buffer, protocol, this);
    }


    @Override
    protected void readCompleted(Integer result, ResponseCallback<AioContextInboud> context) {
        try {
            contextInboud.putByteBuffer(buffer);
            buffer.clear();
        }finally {
            channel.read(buffer, context, this);
        }
    }

    @Override
    protected void onChannelClose(ResponseCallback<AioContextInboud> context) {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore;
        }
        failed(new ClosedChannelException(), context);
    }

    @Override
    public void failed(Throwable cause, ResponseCallback<AioContextInboud> context) {
        context.failed(cause);
    }

}
