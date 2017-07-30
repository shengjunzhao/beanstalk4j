package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.command.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public class BeanstalkReplayReader extends AbstractReadCallback<ResponseCallback<Response>> {

    private CharsetDecoder decoder;
    private ByteBuffer buffer;
    private AsynchronousSocketChannel channel;
    private Response response = new Response();

    public BeanstalkReplayReader(Charset charset, AsynchronousSocketChannel channel) {
        this.decoder = charset.newDecoder();
        this.channel = channel;
    }

    @Override
    protected void readCompleted(Integer result, ResponseCallback<Response> context) {
        ByteBuffer buffer = this.buffer;
        try {
            int position = buffer.position();
            if (buffer.get(position - 2) == 13 && buffer.get(position - 1) == 10){


            }
            if (buffer.hasRemaining()) {
                channel.read(buffer, context, this);
                return;
            }




        }catch (Exception ex) {
            failed(ex, context);
        }

    }

    @Override
    protected void onChannelClose(ResponseCallback<Response> context) {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore;
        }
        failed(new ClosedChannelException(), context);
    }

    @Override
    public void failed(Throwable exc, ResponseCallback<Response> context) {
        context.failed(exc);
    }
}
