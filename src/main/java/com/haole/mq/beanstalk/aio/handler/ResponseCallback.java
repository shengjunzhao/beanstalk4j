package com.haole.mq.beanstalk.aio.handler;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public interface ResponseCallback<T> {
    void onResponse(T respone);

    void failed(Throwable cause);
}
