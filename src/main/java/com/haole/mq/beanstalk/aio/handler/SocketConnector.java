package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.aio.Connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public class SocketConnector implements Connector<Object[]> {
    public void connect(InetSocketAddress remote, ConnectionCallback client) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
        AsynchronousChannelGroup asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(asyncChannelGroup)
                .setOption(StandardSocketOptions.TCP_NODELAY,true)
                .setOption(StandardSocketOptions.SO_REUSEADDR,true)
                .setOption(StandardSocketOptions.SO_KEEPALIVE,true);
        Object[] attachment = {client, remote, channel};
        channel.connect(remote, attachment, this);
    }

    public void connect(InetSocketAddress remote, InetSocketAddress local, ConnectionCallback client)
            throws IOException {
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        channel.bind(local);
        Object[] attachment = {client, remote, channel};
        channel.connect(remote, attachment, this);
    }

    @Override
    public void completed(Void result, Object[] attachment) {
        ((ConnectionCallback) attachment[0]).start((AsynchronousSocketChannel) attachment[2]);
    }

    @Override
    public void failed(Throwable cause, Object[] attachment) {
        ((ConnectionCallback) attachment[0]).connectFailed(new Exception(attachment[1].toString(), cause));
    }


}
