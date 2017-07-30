package com.haole.mq.beanstalk.aio.handler;

import java.nio.channels.AsynchronousSocketChannel;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public interface ConnectionCallback {
    void start(AsynchronousSocketChannel channel);

    void connectFailed(Throwable cause);

}
