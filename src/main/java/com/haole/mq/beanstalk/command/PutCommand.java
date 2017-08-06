package com.haole.mq.beanstalk.command;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * put command
 * Created by shengjunzhao on 2017/5/27.
 */
public class PutCommand extends AbstractCommand {

    private final static Logger log = LoggerFactory.getLogger(PutCommand.class);

    private byte[] data;

    public PutCommand(int priority, int delay, int ttr, byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data is null");
        }
        if (data.length > 65536) {
            throw new IllegalArgumentException("data is too long than 65536");
        }
        setCommandLine("put " + priority + " " + delay + " " + ttr + " " + data.length);
        this.data = data;
    }

    @Override
    public ByteBuf prepareRequest(ByteBuf sendBuf, Charset charset, String delimiter) {
        sendBuf = super.prepareRequest(sendBuf, charset, delimiter);
        sendBuf.writeBytes(data);
        sendBuf.writeBytes(delimiter.getBytes(charset));
        return sendBuf;
    }

    @Override
    public ByteBuffer prepareRequest(ByteBuffer sendBuf, Charset charset, String delimiter) {
        sendBuf = super.prepareRequest(sendBuf, charset, delimiter);
        int remainder = sendBuf.capacity() - sendBuf.position();
        if (data.length > remainder - 2) {
            int capacity = sendBuf.position() + data.length + 2;
            int blockNum = capacity / blockSize;
            capacity = (blockNum + 1) * blockSize;
            ByteBuffer allocBuffer = ByteBuffer.allocateDirect(capacity);
            sendBuf.flip();
            allocBuffer.put(sendBuf);
            sendBuf = allocBuffer;
        }
        sendBuf.put(data);
        sendBuf.put(delimiter.getBytes(charset));
        return sendBuf;
    }
}
