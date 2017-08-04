package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shengjunzhao on 2017/8/4.
 */
public class EventLoop implements ResponseCallback<Response>,WriteCallback {

    private final static Logger log = LoggerFactory.getLogger(EventLoop.class);

    private BeanstalkReplayReader replayReader;
    private BeanstalkBufferWriter bufferWriter;




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
