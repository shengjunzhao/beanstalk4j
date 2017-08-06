package com.haole.mq.beanstalk.command;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public interface Command {

    int blockSize = 512;

    String getCommandLine();

    ByteBuf prepareRequest(ByteBuf sendBuf, Charset charset, String delimiter);

    ByteBuffer prepareRequest(ByteBuffer sendBuf, Charset charset, String delimiter);

}
