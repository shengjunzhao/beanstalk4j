package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.aio.Callback;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public abstract class AbstractReadCallback<T> implements Callback<T> {

    protected abstract void readCompleted(Integer result, T context);

    protected abstract void onChannelClose(T context);

    @Override
    public void completed(Integer result, T context) {
        if (result.intValue() > 0)
            readCompleted(result, context);
        else
            onChannelClose(context);

    }
}
