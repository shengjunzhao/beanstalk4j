package com.haole.mq.beanstalk.codec;

import com.haole.mq.beanstalk.command.Command;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * Created by shengjunzhao on 2017/5/28.
 */
public class CommandEncode extends MessageToByteEncoder<Command> {

    private static final Logger log = LoggerFactory.getLogger(CommandEncode.class);

    private Charset charset;
    private String delimiter;

    public CommandEncode(Charset charset) {
        this(charset, "\r\n");
    }

    public CommandEncode(Charset charset, String delimiter) {
        this.charset = charset;
        this.delimiter = delimiter;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Command msg, ByteBuf out) throws Exception {
        if (null == msg) {
            throw new Exception("The encode message is null");
        }
        ByteBuf sendBuf = ctx.channel().alloc().buffer(512);
        log.debug("&&&&& command={}", msg.getCommandLine());
        sendBuf = msg.prepareRequest(sendBuf, charset, delimiter);
        out.writeBytes(sendBuf);
    }
}
