package com.haole.mq.beanstalk.impl;

import com.haole.mq.beanstalk.Job;
import com.haole.mq.beanstalk.YamlUtil;
import com.haole.mq.beanstalk.codec.CommandDecode;
import com.haole.mq.beanstalk.codec.CommandEncode;
import com.haole.mq.beanstalk.command.*;
import com.haole.mq.beanstalk.handler.BeanstalkHandler;
import com.haole.mq.beanstalk.hash.KetamaConsistentHash;
import com.haole.mq.beanstalk.hash.KetamaHash;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public class DefaultBeanstalkClient {

    private final static Logger log = LoggerFactory.getLogger(DefaultBeanstalkClient.class);

    private EventLoopGroup group = new NioEventLoopGroup();
    private Bootstrap b = new Bootstrap();
    private KetamaConsistentHash consistentHash;

    /**
     * 服务器列表，格式：ip:port
     */
    private Set<String> allServers = new CopyOnWriteArraySet();
    private ConcurrentHashMap<String, Channel> tubePool = new ConcurrentHashMap<>();
    private Lock lock = new ReentrantLock();


    private void init() {
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("beanstalk decode", new CommandDecode(Charset.forName("UTF-8")));
                        pipeline.addLast("beanstalk encode", new CommandEncode(Charset.forName("UTF-8")));
                        pipeline.addLast("beanstalk client handler", new BeanstalkHandler());

                    }
                });

    }

    private static class BeanstalkClientHolder {
        private static DefaultBeanstalkClient instance = new DefaultBeanstalkClient();
    }

    public static DefaultBeanstalkClient getInstance() {
        return BeanstalkClientHolder.instance;
    }

    private DefaultBeanstalkClient() {
        init();
    }

    public void addServers(Set<String> servers) {
        int oldLen = this.allServers.size();
        for (String server : servers) {
            this.allServers.add(server);
        }
        int len = this.allServers.size();
        if (oldLen != len) {
            if (null == consistentHash)
                consistentHash = new KetamaConsistentHash(this.allServers);
            else
                consistentHash.addServers(this.allServers);
        }
    }

    public Channel getChannel(String tube, boolean isProvider) throws InterruptedException {
        try {
            lock.lock();
            String ptube = isProvider ? tube + "_p" : tube + "_c";
            Channel channel = tubePool.get(ptube);
            if (null == channel) {
                String server = getServersOfTube(tube);
                String[] splits = server.split(":");
                String host = splits[0];
                int port = Integer.valueOf(splits[1]).intValue();
                channel = connect(host, port);
                if (null == channel) {
                    this.allServers.remove(server);
                    consistentHash.removeServer(server);
                    return null;
                }
                tubePool.put(ptube, channel);
            }
            return channel;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按照某种规则，依据tube得到服务器地址
     *
     * @param tube
     * @return
     */
    private String getServersOfTube(String tube) {
        long hash = KetamaHash.hash(tube);
        String server = consistentHash.getServerforKey(hash);
        return server;
    }

    public Channel connect(String host, int port) throws InterruptedException {
        ChannelFuture furture = b.connect(new InetSocketAddress(host, port)).sync();
        log.info("{}-{}", furture.channel().remoteAddress(), furture.channel().localAddress());
        furture.channel().closeFuture().addListener((ChannelFuture channelFurture) -> {
            for (Map.Entry<String, Channel> entry : DefaultBeanstalkClient.this.tubePool.entrySet()) {
                if (channelFurture.channel().equals(entry.getValue()))
                    DefaultBeanstalkClient.this.tubePool.remove(entry.getKey());
            }
            if (DefaultBeanstalkClient.this.tubePool.isEmpty()) {
                log.debug("##### group.shutdownGracefully");
                DefaultBeanstalkClient.this.group.shutdownGracefully();
            }
        });

//        furture.channel().closeFuture().addListener(new ChannelFutureListener() {
//
//            @Override
//            public void operationComplete(ChannelFuture future) throws Exception {
//                for (Map.Entry<String, Channel> entry : DefaultBeanstalkClient.this.tubePool.entrySet()) {
//                    if (furture.channel().equals(entry.getValue()))
//                        DefaultBeanstalkClient.this.tubePool.remove(entry.getKey());
//                }
//            }
//        });
        if (furture.isSuccess()) {
            return furture.channel();
        }
        return null;
    }

    /**
     * @param channel
     * @param tube
     * @return
     * @throws InterruptedException
     */
    public boolean use(Channel channel, String tube) throws InterruptedException {
        Command useCommand = new UseCommand(tube);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(useCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("USING");
    }

    /**
     * 向队列中插入一个job
     *
     * @param channel
     * @param priority 优先级 0~2**32的整数，最高优先级是0
     * @param delay    是一个整形数，表示将job放入ready队列需要等待的秒数
     * @param ttr      time to run—是一个整形数，表示允许一个worker执行该job的秒数。这个时间将从一个worker 获取一个job开始计算。
     *                 如果该worker没能在<ttr> 秒内删除、释放或休眠该job，这个job就会超时，服务端会主动释放该job。
     *                 最小ttr为1。如果客户端设置了0，服务端会默认将其增加到1。
     * @param data     及job体，是一个长度为<byetes> 的字符序列
     * @return 如果大于0，是新job的数字编号，如果小于0，错误，-1;未知;-2:BURIED;-3:EXPECTED_CRLF;-4:JOB_TOO_BIG;-5:DRAINING
     * @throws InterruptedException
     */
    public long put(Channel channel, int priority, int delay, int ttr, byte[] data) throws InterruptedException {
        Command putCommand = new PutCommand(priority, delay, ttr, data);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(putCommand);
        log.debug("response status {}", response.getStatusLine());
        String[] spilts = response.getStatusLine().split(" ");
        if ("INSERTED".equals(spilts[0])) {
            return Long.valueOf(spilts[1]).longValue();
        } else if ("BURIED".equals(spilts[0])) {
            return -2;
        } else if ("EXPECTED_CRLF".equals(spilts[0])) {
            return -3;
        } else if ("JOB_TOO_BIG".equals(spilts[0])) {
            return -4;
        } else if ("DRAINING".equals(spilts[0])) {
            return -5;
        } else
            return -1;
    }

    /**
     * 从队列得到一个job
     *
     * @param channel
     * @param timeout 秒
     * @return
     */
    public Job reserve(Channel channel, long timeout) throws InterruptedException {
        Command reserveCommand = new ReserveCommand(timeout);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(reserveCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("RESERVED")) {
            String[] spilts = response.getStatusLine().split(" ");
            Job job = new Job();
            job.setId(Long.valueOf(spilts[1]).longValue());
            job.setData(response.getData());
            return job;
        }
        return null;
    }

    /**
     * 从服务端完全删除一个job
     *
     * @param channel
     * @param id      jodId
     * @return
     */
    public boolean delete(Channel channel, long id) throws InterruptedException {
        Command deleteCommand = new DeleteCommand(id);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(deleteCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("DELETED");
    }

    /**
     * 将一个已经被获取的job重新放回ready队列（并将job状态置为 “ready”），
     * 让该job可以被其他客户端执行。这个命令经常在job因为短暂的错误而失败时使用
     *
     * @param channel
     * @param id
     * @param priority
     * @param delay
     * @return
     * @throws InterruptedException
     */
    public boolean release(Channel channel, long id, int priority, int delay) throws InterruptedException {
        Command releaseCommand = new ReleaseCommand(id, priority, delay);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(releaseCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("RELEASED");
    }

    /**
     * 令将一个job的状态置为”buried”。Buried job被放在一个FIFO的链表中，在客户端调用kick命令之前，这些job将不会被服务端处理
     *
     * @param channel
     * @param id
     * @param priority
     * @return
     * @throws InterruptedException
     */
    public boolean bury(Channel channel, long id, int priority) throws InterruptedException {
        Command buryCommand = new BuryCommand(id, priority);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(buryCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("BURIED");
    }

    /**
     * 允许一个worker请求在一个job获取更多执行的时间
     * 在收到DEADLINE_SOON是可以发生给命令
     *
     * @param channel
     * @param id
     * @return
     * @throws InterruptedException
     */
    public boolean touch(Channel channel, long id) throws InterruptedException {
        Command touchCommand = new TouchCommand(id);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(touchCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("TOUCHED");
    }

    /**
     * 将一个tube名称加入当前连接的watch list。reserve命令将从watch list里面的任意一个tube中获取job
     *
     * @param channel
     * @param tube
     * @return
     * @throws InterruptedException
     */
    public boolean watch(Channel channel, String tube) throws InterruptedException {
        Command watchCommand = new WatchCommand(tube);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(watchCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    /**
     * 为consumer设计的。ignore命令将一个tube从当前连接的watch list中移除
     *
     * @param channel
     * @param tube
     * @return
     * @throws InterruptedException
     */
    public boolean ignore(Channel channel, String tube) throws InterruptedException {
        Command ignoreCommand = new IgnoreCommand(tube);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(ignoreCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    /**
     * @param channel
     * @param peekCommand
     * @return
     * @throws InterruptedException
     */
    private Job peek(Channel channel, PeekCommand peekCommand) throws InterruptedException {
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(peekCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("FOUND")) {
            String[] spilts = response.getStatusLine().split(" ");
            Job job = new Job();
            job.setId(Long.valueOf(spilts[1]).longValue());
            job.setData(response.getData());
            return job;
        }
        return null;
    }

    /**
     * 让客户端检查系统中的job
     *
     * @param channel
     * @param id
     * @return
     * @throws InterruptedException
     */
    public Job peek(Channel channel, long id) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(id);
        return peek(channel, peekCommand);
    }

    /**
     * 在当前使用的tube上面, 返回当前tube下一个ready的job
     *
     * @param channel
     * @return
     * @throws InterruptedException
     */
    public Job peekReady(Channel channel) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.ready);
        return peek(channel, peekCommand);
    }

    /**
     * 在当前使用的tube上面, 返回当前tube剩余delay时间最短的job
     *
     * @return
     * @throws InterruptedException
     */
    public Job peekDelayed(Channel channel) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.delayed);
        return peek(channel, peekCommand);
    }

    /**
     * 在当前使用的tube上面, 返回当前tube buried list中下一个job
     *
     * @param channel
     * @return
     * @throws InterruptedException
     */
    public Job peekBuried(Channel channel) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.buried);
        return peek(channel, peekCommand);
    }

    /**
     * 针对当前正在使用的tube执行。它将buried或者delayed状态的job移动到ready队列
     *
     * @param channel
     * @param bound   每次kick job的上限
     * @return
     * @throws InterruptedException
     */
    public boolean kick(Channel channel, long bound) throws InterruptedException {
        Command kickCommand = new KickCommand(bound);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(kickCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }


    /**
     * @param channel
     * @param id
     * @return
     * @throws InterruptedException
     */
    public boolean kickJob(Channel channel, long id) throws InterruptedException {
        Command kickCommand = new KickJobCommand(id);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(kickCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }

    /**
     * 返回已经存在的所有tube的列表
     *
     * @param channel
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public List<String> listTubes(Channel channel) throws InterruptedException, IOException {
        Command listTubesCommand = new ListTubesCommand();
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(listTubesCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    /**
     * 返回客户当前正在使用的tube
     *
     * @param channel
     * @return
     * @throws InterruptedException
     */
    public String listTubeUsed(Channel channel) throws InterruptedException {
        Command listTubeUsedCommand = new ListTubeUsedCommand();
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(listTubeUsedCommand);
        log.debug("response status {}", response.getStatusLine());
        String[] splits = response.getStatusLine().split(" ");
        return splits[splits.length - 1];
    }

    /**
     * 返回客户端当前正在关注的tube名称列表
     *
     * @param channel
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public List<String> listTubesWatched(Channel channel) throws InterruptedException, IOException {
        Command listTubesWatchedCommand = new ListTubesWatchedCommand();
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(listTubesWatchedCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    /**
     * 在给定时间内暂停从tube获取job
     *
     * @param channel
     * @param tube
     * @param delay
     * @return
     * @throws InterruptedException
     */
    public boolean pauseTube(Channel channel, String tube, int delay) throws InterruptedException {
        Command pauseTubeCommand = new PauseTubeCommand(tube, delay);
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(pauseTubeCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("PAUSED");
    }

    /**
     * @param channel
     * @param statsCommand
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    private Map<String, String> stats(Channel channel, StatsCommand statsCommand) throws InterruptedException, IOException {
        Response response = channel.pipeline().get(BeanstalkHandler.class).sendMessage(statsCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2Map(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    /**
     * 返回整个消息队列系统的整体信息
     *
     * @param channel
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Map<String, String> stats(Channel channel) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand();
        return stats(channel, statsCommand);
    }

    /**
     * 统计job的相关信息
     *
     * @param channel
     * @param id
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Map<String, String> statsJob(Channel channel, long id) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(id);
        return stats(channel, statsCommand);
    }

    /**
     * 统计tube的相关信息
     *
     * @param channel
     * @param tube
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Map<String, String> statsTube(Channel channel, String tube) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(tube);
        return stats(channel, statsCommand);
    }


    /**
     * 关闭当前连接
     *
     * @param channel
     * @throws InterruptedException
     */
    public void quit(Channel channel) throws InterruptedException {
        Command quitCommand = new QuitCommand();
        ChannelFuture quitFuture = channel.writeAndFlush(quitCommand).sync();
        ChannelFuture closeFuture = channel.close();
    }


    public void destroy() throws InterruptedException {
        for (Map.Entry<String, Channel> tubeKey : tubePool.entrySet())
            quit(tubeKey.getValue());
        group.shutdownGracefully();
    }


}
