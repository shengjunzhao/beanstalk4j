package com.haole.mq.beanstalk.aio.handler;

import com.haole.mq.beanstalk.command.Command;
import com.haole.mq.beanstalk.command.QuitCommand;
import com.haole.mq.beanstalk.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by shengjunzhao on 2017/8/4.
 */
public class BeanstalkChannel implements ResponseCallback<Response>, WriteCallback {

    private final static Logger log = LoggerFactory.getLogger(BeanstalkChannel.class);

    private BeanstalkReplayReader replayReader;
    private AioSocketChannelWriter channelWriter;
    private LinkedBlockingQueue<Response> queue = new LinkedBlockingQueue<>();
    private Charset charset = Charset.forName("UTF-8");
    private String delimiter = "\r\n";
    private Semaphore semaphore = new Semaphore(0);
    private AtomicBoolean isConnected = new AtomicBoolean(false);


    public void connect(String host, int port) throws IOException {
        connect(new InetSocketAddress(host, port));
    }

    public void connect(InetSocketAddress remote) throws IOException {
        SocketConnector connector = new SocketConnector();
        connector.connect(remote, new ConnectionCallback() {
            @Override
            public void start(AsynchronousSocketChannel channel) {
                BeanstalkChannel.this.start(channel);
            }

            @Override
            public void connectFailed(Throwable cause) {
                log.error("connect failed because: {}", cause);
                System.exit(-1);
            }
        });
    }

    public Response write(Command command) throws InterruptedException {
        while (!isConnected.get()) {}
//        semaphore.acquire();

        ByteBuffer buffer = ByteBuffer.allocateDirect(Command.blockSize);
        buffer = command.prepareRequest(buffer, charset, delimiter);
        log.debug("&&&&& command={}", command.getCommandLine());
        channelWriter.write(buffer, this);
        Response response = queue.take();
        return response;
    }

    public void quit() {
        Command quitCommand = new QuitCommand();
        ByteBuffer buffer = ByteBuffer.allocateDirect(Command.blockSize);
        buffer = quitCommand.prepareRequest(buffer, charset, delimiter);
        channelWriter.write(buffer, this);
        try {
            channelWriter.close();
            replayReader.close();
        } catch (IOException e) {
            log.error("close channel:{}", e);
        }

    }


    protected void start(AsynchronousSocketChannel channel) {
        InetSocketAddress remote;
        try {
            remote = (InetSocketAddress) channel.getRemoteAddress();
            log.info("remote address: {}", remote);
        } catch (IOException e) {
            failed(e);
            return;
        }
        InetSocketAddress local;
        try {
            local = (InetSocketAddress) channel.getLocalAddress();
            log.info("local address: {}", local);
        } catch (IOException e) {
            failed(e);
            return;
        }
        charset = Charset.forName("UTF-8");
        replayReader = new BeanstalkReplayReader(charset, channel);
        channelWriter = new AioSocketChannelWriter(channel);
//        replayReader.read(this);
//        semaphore.release();
        isConnected.set(true);
    }


    @Override
    public void writeCompleted() {
        replayReader.read(this);
    }

    @Override
    public void writeFailed(Throwable cause) {
        log.error("Write failed because: {}", cause);
    }

    @Override
    public void onResponse(Response respone) {
        try {
            queue.put(respone);
//            semaphore.release();
        } catch (InterruptedException e) {
            log.error("read reponse {}", e);
        }
    }

    @Override
    public void failed(Throwable cause) {
        log.error("Read failed because: {}", cause);
    }
}
