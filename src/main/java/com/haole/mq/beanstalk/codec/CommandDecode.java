package com.haole.mq.beanstalk.codec;

import com.haole.mq.beanstalk.command.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by shengjunzhao on 2017/5/28.
 */
public class CommandDecode extends ByteToMessageDecoder {

    private final static Logger log = LoggerFactory.getLogger(CommandDecode.class);
    private Charset charset;
    private String delimiter;

    public CommandDecode(Charset charset) {
        this(charset, "\r\n");
    }

    public CommandDecode(Charset charset, String delimiter) {
        this.charset = charset;
        this.delimiter = delimiter;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        int readableBytes = in.readableBytes();
        Response response = new Response();
        byte[] resp = new byte[readableBytes];
        in.readBytes(resp);
        log.debug("bytebuf in {}",resp);
        byte previous = 0;
        boolean isReset = true;
        for (int i = 0; i < readableBytes; i++) {
            byte current = resp[i];
            if (previous == 13 && current == 10) {
                String commandLine = new String(resp, 0, i - 1, charset);
                String[] spilts = commandLine.split(" ");
                String result = spilts[0];
                if ("RESERVED".equals(result) || "FOUND".equals(result) || "OK".equals(result)) {
                    String bytesStr = spilts[spilts.length - 1];
                    if (bytesStr.matches("\\d+")) {
                        int bytes = Integer.valueOf(bytesStr);
                        if (bytes == readableBytes - i - 1 - 2) {
                            byte[] data = new byte[bytes];
                            System.arraycopy(resp, i + 1, data, 0, bytes);
                            response.setData(data);
                            isReset = false;
                        }
                    } else
                        isReset = false;
                } else
                    isReset = false;
                response.setStatusLine(commandLine);
                break;
            }
            previous = current;
        }
        if (isReset)
            in.resetReaderIndex();
        else {
            out.add(response);
        }
    }
}
