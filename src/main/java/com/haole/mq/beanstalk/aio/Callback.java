package com.haole.mq.beanstalk.aio;

import java.nio.channels.CompletionHandler;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public interface Callback<T> extends CompletionHandler<Integer, T> {

    @Override
    void completed(Integer result, T context);

    @Override
    void failed(Throwable exc, T context);
}
