package com.haole.mq.beanstalk.command;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public interface Command {

    String getCommandLine();

    ByteBuf prepareRequest(ByteBuf sendBuf, Charset charset, String delimiter);

}
