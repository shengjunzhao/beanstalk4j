package com.haole.mq.beanstalk.aio.channel;

import com.haole.mq.beanstalk.aio.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by shengjunzhao on 2017/8/12.
 */
public class AioSocketChannelEventLoop {

    private static final Logger log = LoggerFactory.getLogger(AioSocketChannelEventLoop.class);

    private AsynchronousSocketChannel channel;
    private AtomicBoolean isConnected = new AtomicBoolean(false);
    private AioSocketChannelReader channelReader;
    private AioSocketChannelWriter channelWriter;
    private ResponseCallback<AioContextInboud> responseCallback;
    private WriteCallback writeCallback;
    private Semaphore semaphore = new Semaphore(0);

    public AioSocketChannelEventLoop connect(String host, int port) throws IOException, InterruptedException {
        return connect(new InetSocketAddress(host, port));
    }

    public AioSocketChannelEventLoop connect(InetSocketAddress remote) throws IOException, InterruptedException {
        if (isConnected.get())
            return this;
        SocketConnector connector = new SocketConnector();
        connector.connect(remote, new ConnectionCallback() {
            @Override
            public void start(AsynchronousSocketChannel channel) {
                AioSocketChannelEventLoop.this.start(channel);
            }

            @Override
            public void connectFailed(Throwable cause) {
                log.error("connect failed because: {}", cause);
                System.exit(-1);
            }
        });
        semaphore.acquire();
        return this;
    }

    protected void start(AsynchronousSocketChannel channel) {
        InetSocketAddress remote;
        try {
            remote = (InetSocketAddress) channel.getRemoteAddress();
            log.info("remote address: {}", remote);
        } catch (IOException e) {
            failed(e);
            return;
        }
        InetSocketAddress local;
        try {
            local = (InetSocketAddress) channel.getLocalAddress();
            log.info("local address: {}", local);
        } catch (IOException e) {
            failed(e);
            return;
        }
        this.channel = channel;
        channelReader = new AioSocketChannelReader(channel, this);
        channelReader.read(responseCallback);
        channelWriter = new AioSocketChannelWriter(channel);
        isConnected.set(true);
        semaphore.release();
    }

    public void write(ByteBuffer buffer) {
        channelWriter.write(buffer, writeCallback);
    }

    public AtomicBoolean getIsConnected() {
        return isConnected;
    }

    public void setIsConnected(boolean isConnected) {
        this.isConnected.set(isConnected);
    }

    public AioSocketChannelEventLoop setResponseCallback(ResponseCallback<AioContextInboud> inboud) {
        this.responseCallback = inboud;
        return this;
    }

    public AioSocketChannelEventLoop setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
        return this;
    }

    public void failed(Throwable cause) {
        log.error("Read failed because: {}", cause);
    }

    public AsynchronousSocketChannel getChannel() {
        return channel;
    }

    public ResponseCallback<AioContextInboud> getResponseCallback() {
        return responseCallback;
    }

    public WriteCallback getWriteCallback() {
        return writeCallback;
    }

    public void close() {
        if (channel.isOpen())
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
}
