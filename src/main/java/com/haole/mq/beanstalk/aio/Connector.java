package com.haole.mq.beanstalk.aio;

import java.nio.channels.CompletionHandler;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public interface Connector<T> extends CompletionHandler<Void, T> {

    @Override
    void completed(Void result, T attachment);

    @Override
    void failed(Throwable exc, T attachment);
}
