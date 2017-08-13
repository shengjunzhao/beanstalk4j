package com.haole.mq.beanstalk.aio.code;

import com.haole.mq.beanstalk.aio.channel.AioContextInboud;
import com.haole.mq.beanstalk.aio.handler.ResponseCallback;
import com.haole.mq.beanstalk.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by shengjunzhao on 2017/8/13.
 */
public class BeanstalkReadCallback implements ResponseCallback<AioContextInboud> {

    private static final Logger log = LoggerFactory.getLogger(BeanstalkReadCallback.class);
    private LinkedBlockingQueue<Response> queue = new LinkedBlockingQueue<>();
    private Response response = new Response();
    private Charset decoder;

    public BeanstalkReadCallback(Charset charset) {
        this.decoder = charset;
    }

    @Override
    public void onResponse(AioContextInboud contextInboud) {
        ByteBuffer buffer = contextInboud.getBuffer();
        try {
            int position = buffer.position();
            byte previous = 0;
            for (int i = 0; i < position; i++) {
                byte current = buffer.get(i);
                if (previous == 13 && current == 10) { //  && "".equals(response.getStatusLine())
                    // 不考虑data中包括回车换行
                    if (!"".equals(response.getStatusLine())) {
                        Response resultResponse = response.clone();
                        queue.put(resultResponse);
                        response.reset();
                    }
                    buffer.flip();
                    byte[] commandByte = buffer2byte(buffer, 0, i - 1);
                    String commandLine = new String(commandByte, decoder);
                    log.debug("receive command: {}", commandLine);
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
                                queue.put(resultResponse);
                                return;
                            } else {
                                fillResponseData(buffer, i + 1, position - i - 1);
                                buffer.clear();
                                return;
                            }
                        } else { // 不应该出现这种情况
                            buffer.compact();
                            Response resultResponse = response.clone();
                            queue.put(resultResponse);
                            response.reset();
                            return;
                        }
                    } else {
                        buffer.compact();
                        Response resultResponse = response.clone();
                        queue.put(resultResponse);
                        response.reset();
                        return;
                    }
                }
                previous = current;
            }
            buffer.flip();
            fillResponseData(buffer, 0, buffer.limit());
            buffer.clear();
        } catch (Exception ex) {
            failed(ex);
        }


    }

    @Override
    public void failed(Throwable cause) {
        log.error("read reponse callback error:{}", cause);
    }

    public LinkedBlockingQueue<Response> getQueue() {
        return this.queue;
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
}
