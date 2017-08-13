package com.haole.mq.beanstalk.aio.code;

import com.haole.mq.beanstalk.aio.channel.AioContextInboud;
import com.haole.mq.beanstalk.aio.handler.ResponseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by shengjunzhao on 2017/8/13.
 */
public class BeantalkReadCallback implements ResponseCallback<AioContextInboud> {

    private static final Logger log = LoggerFactory.getLogger(BeantalkReadCallback.class);

    @Override
    public void onResponse(AioContextInboud respone) {
        ByteBuffer buffer = respone.getBuffer();

    }

    @Override
    public void failed(Throwable cause) {
        log.error("read reponse callback error:{}",cause);
    }
}
