package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * Created by shengjunzhao on 2017/7/30.
 */
public class BeanstalkReplayReader extends AbstractReadCallback<ResponseCallback<Response>> {

    private static final Logger log = LoggerFactory.getLogger(BeanstalkReplayReader.class);

    private Charset decoder;
    private ByteBuffer buffer;
    private AsynchronousSocketChannel channel;
    private Response response = new Response();

    public BeanstalkReplayReader(Charset charset, AsynchronousSocketChannel channel) {
        this.decoder = charset;
        this.channel = channel;
    }

    public void read(ResponseCallback<Response> protocol) {
        response.reset();
        if (buffer == null)
            buffer = ByteBuffer.allocateDirect(1024);
        buffer.clear();
        channel.read(buffer, protocol, this);
    }

    @Override
    protected void readCompleted(Integer result, ResponseCallback<Response> context) {
        ByteBuffer buffer = this.buffer;
        try {
            int position = buffer.position();
            byte previous = 0;
            for (int i = 0; i < position; i++) {
                byte current = buffer.get(i);
                if (previous == 13 && current == 10) { //  && "".equals(response.getStatusLine())
                    // 不考虑data中包括回车换行
                    if (!"".equals(response.getStatusLine())) {
                        Response resultResponse = response.clone();
                        context.onResponse(resultResponse);
                        response.reset();
                    }
                    buffer.flip();
                    byte[] commandByte = buffer2byte(buffer, 0, i - 1);
                    String commandLine = new String(commandByte, decoder);
                    log.debug("receive command: {}",commandLine);
                    response.setStatusLine(commandLine);
                    String[] spilts = commandLine.split(" ");
                    String resultInfo = spilts[0];
                    if ("RESERVED".equals(resultInfo) || "FOUND".equals(resultInfo) || "OK".equals(resultInfo)) {
                        String bytesStr = spilts[spilts.length - 1];
                        if (bytesStr.matches("\\d+")) {
                            int bytes = Integer.valueOf(bytesStr);
                            if (bytes <= position - i - 1 - 2) {
                                byte rn = buffer.get();
                                rn = buffer.get();
                                fillResponseData(buffer, i + 1, bytes);
                                Response resultResponse = response.clone();
                                response.reset();
                                fillResponseData(buffer, i + 1 + bytes, position - 1 - i - 1 - bytes);
                                buffer.clear();
                                context.onResponse(resultResponse);
                                // channel.read(buffer, context, this);
                                return;
                            } else {
                                fillResponseData(buffer, i + 1, position - i - 1);
                                buffer.clear();
                                channel.read(buffer, context, this);
                                return;
                            }
                        } else { // 不应该出现这种情况
                            buffer.compact();
                            context.onResponse(response);
                            return;
                        }
                    } else {
                        buffer.compact();
                        context.onResponse(response);
                        return;
                    }
                }
                previous = current;
            }
            if (buffer.hasRemaining()) {
                channel.read(buffer, context, this);
                return;
            }
            buffer.flip();
            fillResponseData(buffer, 0, buffer.limit());
            buffer.clear();
            channel.read(buffer, context, this);
        } catch (Exception ex) {
            failed(ex, context);
        }
    }

    private byte[] buffer2byte(ByteBuffer buffer, int offset, int len) {
        byte[] bytes = new byte[len];
//        int end = offset + len;
//        for (int i = offset; i < end; i++)
//            bytes[i - offset] = buffer.get(i);
        buffer.get(bytes, 0, len);
        return bytes;
    }

    private void fillResponseData(ByteBuffer buffer, int offset, int len) {
        byte[] data = buffer2byte(buffer, offset, len);
        byte[] allData = new byte[response.getData().length + data.length];
        System.arraycopy(response.getData(), 0, allData, 0, response.getData().length);
        System.arraycopy(data, 0, allData, response.getData().length, data.length);
        response.setData(allData);
    }

    @Override
    protected void onChannelClose(ResponseCallback<Response> context) {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore;
        }
        failed(new ClosedChannelException(), context);
    }

    @Override
    public void failed(Throwable cause, ResponseCallback<Response> context) {
        context.failed(cause);
    }

    public void close() throws IOException {
        if (channel.isOpen())
            channel.close();
    }
}
