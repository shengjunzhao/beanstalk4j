package com.haole.mq.beanstalk.aio.code;

import com.haole.mq.beanstalk.aio.handler.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shengjunzhao on 2017/8/13.
 */
public class BeanstalkWriteCallback implements WriteCallback {

    private static final Logger log = LoggerFactory.getLogger(BeanstalkWriteCallback.class);

    @Override
    public void writeCompleted() {
        log.info("write completed");
    }

    @Override
    public void writeFailed(Throwable cause) {
        log.error("write failed:{}", cause);

    }
}
