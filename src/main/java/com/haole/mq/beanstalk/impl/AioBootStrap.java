package com.haole.mq.beanstalk.impl;

import com.haole.mq.beanstalk.Job;
import com.haole.mq.beanstalk.YamlUtil;
import com.haole.mq.beanstalk.aio.channel.AioSocketChannelEventLoop;
import com.haole.mq.beanstalk.aio.code.BeanstalkReadCallback;
import com.haole.mq.beanstalk.aio.code.BeanstalkWriteCallback;
import com.haole.mq.beanstalk.command.*;
import com.haole.mq.beanstalk.hash.KetamaConsistentHash;
import com.haole.mq.beanstalk.hash.KetamaHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shengjunzhao on 2017/8/13.
 */
public class AioBootStrap {

    private static final Logger log = LoggerFactory.getLogger(AioBootStrap.class);

    private Charset charset = Charset.forName("UTF-8");
    private String delimiter = "\r\n";
    private KetamaConsistentHash consistentHash;

    /**
     * 服务器列表，格式：ip:port
     */
    private Set<String> allServers = new CopyOnWriteArraySet();
    private ConcurrentHashMap<String, AioSocketChannelEventLoop> tubePool = new ConcurrentHashMap<>();
    private Lock lock = new ReentrantLock();

    private AioBootStrap() {

    }

    private static class AioBootStrapHolder {
        private static AioBootStrap instance = new AioBootStrap();
    }

    public static AioBootStrap getInstance() {
        return AioBootStrapHolder.instance;
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

    public AioSocketChannelEventLoop getChannel(String tube, boolean isProvider) throws InterruptedException, IOException {
        try {
            lock.lock();
            String ptube = isProvider ? tube + "_p" : tube + "_c";
            AioSocketChannelEventLoop channel = tubePool.get(ptube);
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

    public AioSocketChannelEventLoop connect(String host, int port) throws IOException, InterruptedException {
        AioSocketChannelEventLoop eventLoop = new AioSocketChannelEventLoop();
        eventLoop.setResponseCallback(new BeanstalkReadCallback(charset)).setWriteCallback(new BeanstalkWriteCallback());
        eventLoop = eventLoop.connect(host, port);
        return eventLoop;
    }

    public ByteBuffer write(Command command) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(Command.blockSize);
        buffer = command.prepareRequest(buffer, charset, delimiter);
        log.debug("&&&&& command={}", command.getCommandLine());
        return buffer;
    }

    public boolean use(AioSocketChannelEventLoop eventLoop, String tube) throws InterruptedException {
        Command useCommand = new UseCommand(tube);
        ByteBuffer buffer = write(useCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("USING");
    }

    public long put(AioSocketChannelEventLoop eventLoop, int priority, int delay, int ttr, byte[] data) throws InterruptedException {
        Command putCommand = new PutCommand(priority, delay, ttr, data);
        ByteBuffer buffer = write(putCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
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

    public Job reserve(AioSocketChannelEventLoop eventLoop, long timeout) throws InterruptedException {
        Command reserveCommand = new ReserveCommand(timeout);
        ByteBuffer buffer = write(reserveCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
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

    public boolean delete(AioSocketChannelEventLoop eventLoop, long id) throws InterruptedException {
        Command deleteCommand = new DeleteCommand(id);
        ByteBuffer buffer = write(deleteCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("DELETED");
    }

    public boolean release(AioSocketChannelEventLoop eventLoop, long id, int priority, int delay) throws InterruptedException {
        Command releaseCommand = new ReleaseCommand(id, priority, delay);
        ByteBuffer buffer = write(releaseCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("RELEASED");
    }

    public boolean bury(AioSocketChannelEventLoop eventLoop, long id, int priority) throws InterruptedException {
        Command buryCommand = new BuryCommand(id, priority);
        ByteBuffer buffer = write(buryCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("BURIED");
    }

    public boolean touch(AioSocketChannelEventLoop eventLoop, long id) throws InterruptedException {
        Command touchCommand = new TouchCommand(id);
        ByteBuffer buffer = write(touchCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("TOUCHED");
    }

    public boolean watch(AioSocketChannelEventLoop eventLoop, String tube) throws InterruptedException {
        Command watchCommand = new WatchCommand(tube);
        ByteBuffer buffer = write(watchCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    public boolean ignore(AioSocketChannelEventLoop eventLoop, String tube) throws InterruptedException {
        Command ignoreCommand = new IgnoreCommand(tube);
        ByteBuffer buffer = write(ignoreCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    private Job peek(AioSocketChannelEventLoop eventLoop, PeekCommand peekCommand) throws InterruptedException {
        ByteBuffer buffer = write(peekCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
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

    public Job peek(AioSocketChannelEventLoop eventLoop, long id) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(id);
        return peek(eventLoop, peekCommand);
    }

    public Job peekReady(AioSocketChannelEventLoop eventLoop) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.ready);
        return peek(eventLoop, peekCommand);
    }

    public Job peekDelayed(AioSocketChannelEventLoop eventLoop) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.delayed);
        return peek(eventLoop, peekCommand);
    }

    public Job peekBuried(AioSocketChannelEventLoop eventLoop) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.buried);
        return peek(eventLoop, peekCommand);
    }

    public boolean kick(AioSocketChannelEventLoop eventLoop, long bound) throws InterruptedException {
        Command kickCommand = new KickCommand(bound);
        ByteBuffer buffer = write(kickCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }

    public boolean kickJob(AioSocketChannelEventLoop eventLoop, long id) throws InterruptedException {
        Command kickCommand = new KickJobCommand(id);
        ByteBuffer buffer = write(kickCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }

    public List<String> listTubes(AioSocketChannelEventLoop eventLoop) throws InterruptedException, IOException {
        Command listTubesCommand = new ListTubesCommand();
        ByteBuffer buffer = write(listTubesCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public String listTubeUsed(AioSocketChannelEventLoop eventLoop) throws InterruptedException {
        Command listTubeUsedCommand = new ListTubeUsedCommand();
        ByteBuffer buffer = write(listTubeUsedCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        String[] splits = response.getStatusLine().split(" ");
        return splits[splits.length - 1];
    }

    public List<String> listTubesWatched(AioSocketChannelEventLoop eventLoop) throws InterruptedException, IOException {
        Command listTubesWatchedCommand = new ListTubesWatchedCommand();
        ByteBuffer buffer = write(listTubesWatchedCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public boolean pauseTube(AioSocketChannelEventLoop eventLoop, String tube, int delay) throws InterruptedException {
        Command pauseTubeCommand = new PauseTubeCommand(tube, delay);
        ByteBuffer buffer = write(pauseTubeCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("PAUSED");
    }

    private Map<String, String> stats(AioSocketChannelEventLoop eventLoop, StatsCommand statsCommand) throws InterruptedException, IOException {
        ByteBuffer buffer = write(statsCommand);
        eventLoop.write(buffer);
        BeanstalkReadCallback callback = (BeanstalkReadCallback) eventLoop.getResponseCallback();
        Response response = callback.getQueue().take();
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2Map(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public Map<String, String> stats(AioSocketChannelEventLoop eventLoop) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand();
        return stats(eventLoop, statsCommand);
    }

    public Map<String, String> statsJob(AioSocketChannelEventLoop eventLoop, long id) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(id);
        return stats(eventLoop, statsCommand);
    }

    public Map<String, String> statsTube(AioSocketChannelEventLoop eventLoop, String tube) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(tube);
        return stats(eventLoop, statsCommand);
    }

    public void quit(AioSocketChannelEventLoop eventLoop) throws InterruptedException {
        Command quitCommand = new QuitCommand();
        ByteBuffer buffer = write(quitCommand);
        eventLoop.write(buffer);
        eventLoop.close();
    }


}
