package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.aio.Callback;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public class BufferWriter implements Callback<WriteCallback> {

    private AsynchronousSocketChannel channel;
    private ByteBuffer buffer;
    private Charset charset;

    public BufferWriter(AsynchronousSocketChannel channel, Charset charset) {
        this.channel = channel;
        this.charset = charset;
    }

    @Override
    public void completed(Integer result, WriteCallback context) {

    }

    @Override
    public void failed(Throwable exc, WriteCallback context) {

    }
}
