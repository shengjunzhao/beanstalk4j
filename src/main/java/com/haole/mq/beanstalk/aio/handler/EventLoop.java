package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * Created by shengjunzhao on 2017/8/4.
 */
public class EventLoop implements ResponseCallback<Response>,WriteCallback {

    private final static Logger log = LoggerFactory.getLogger(EventLoop.class);

    private BeanstalkReplayReader replayReader;
    private BeanstalkBufferWriter bufferWriter;


    public void connect(String host, int port) {

    }
    public void connect(InetSocketAddress remote) throws IOException {
        SocketConnector connector = new SocketConnector();
        connector.connect(remote, new ConnectionCallback() {
            @Override
            public void start(AsynchronousSocketChannel channel) {

            }

            @Override
            public void connectFailed(Throwable cause) {

            }
        });


    }


    @Override
    public void writeCompleted() {

    }

    @Override
    public void writeFailed(Throwable cause) {

    }

    @Override
    public void onResponse(Response respone) {

    }

    @Override
    public void failed(Throwable cause) {

    }
}
