package com.haole.mq.beanstalk.aio.channel;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by shengjunzhao on 2017/8/12.
 */
public class AioSocketChannel {

    private AsynchronousSocketChannel channel;
    private AtomicBoolean isConnected = new AtomicBoolean(false);

}
