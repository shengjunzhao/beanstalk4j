package com.haole.mq.beanstalk.aio.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by shengjunzhao on 2017/8/12.
 */
public class AioSocketChannel {

    private static final Logger log = LoggerFactory.getLogger(AioSocketChannel.class);
    private AsynchronousSocketChannel channel;
    private AtomicBoolean isConnected = new AtomicBoolean(false);



}
