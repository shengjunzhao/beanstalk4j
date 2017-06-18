package com.haole.mq.beanstalk.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public class AbstractCommand implements Command {

    private String commandLine;


    @Override
    public String getCommandLine() {
        return this.commandLine;
    }

    @Override
    public ByteBuf prepareRequest(ByteBuf sendBuf,Charset charset, String delimiter) {
        sendBuf.writeBytes(this.commandLine.getBytes(charset));
        sendBuf.writeBytes(delimiter.getBytes(charset));
        return sendBuf;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }
}
