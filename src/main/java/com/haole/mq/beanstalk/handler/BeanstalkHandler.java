package com.haole.mq.beanstalk.handler;

import com.haole.mq.beanstalk.command.Command;
import com.haole.mq.beanstalk.command.Response;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by shengjunzhao on 2017/5/28.
 */
public class BeanstalkHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BeanstalkHandler.class);
    private LinkedBlockingQueue<Response> queue = new LinkedBlockingQueue<>();
    private Channel channel;



    public BeanstalkHandler() {}

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.channel=ctx.channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Response response = (Response) msg;
        queue.put(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    public Response sendMessage(Command command) throws InterruptedException {
        this.channel.writeAndFlush(command);
        return queue.take();
    }
}
