package com.haole.mq.beanstalk.aio.handler;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public interface WriteCallback {
    void writeCompleted();

    void writeFailed(Throwable cause);

}
