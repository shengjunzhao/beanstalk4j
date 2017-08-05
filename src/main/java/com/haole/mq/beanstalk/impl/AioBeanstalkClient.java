package com.haole.mq.beanstalk.impl;

import com.haole.mq.beanstalk.Job;
import com.haole.mq.beanstalk.YamlUtil;
import com.haole.mq.beanstalk.aio.handler.BeanstalkChannel;
import com.haole.mq.beanstalk.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


/**
 * Created by shengjunzhao on 2017/8/5.
 */
public class AioBeanstalkClient {

    private static final Logger log = LoggerFactory.getLogger(AioBeanstalkClient.class);

    private BeanstalkChannel eventLoop;

    public AioBeanstalkClient(InetSocketAddress remote) throws IOException {
        this.eventLoop = new BeanstalkChannel();
        eventLoop.connect(remote);
    }

    public AioBeanstalkClient(String host, int port) throws IOException {
        this.eventLoop = new BeanstalkChannel();
        eventLoop.connect(host, port);
    }

    public boolean use(String tube) throws InterruptedException {
        Command useCommand = new UseCommand(tube);
        Response response = eventLoop.write(useCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("USING");
    }

    public long put(int priority, int delay, int ttr, byte[] data) throws InterruptedException {
        Command putCommand = new PutCommand(priority, delay, ttr, data);
        Response response = eventLoop.write(putCommand);
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

    public Job reserve(long timeout) throws InterruptedException {
        Command reserveCommand = new ReserveCommand(timeout);
        Response response = eventLoop.write(reserveCommand);
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

    public boolean delete(long id) throws InterruptedException {
        Command deleteCommand = new DeleteCommand(id);
        Response response = eventLoop.write(deleteCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("DELETED");
    }

    public boolean release(long id, int priority, int delay) throws InterruptedException {
        Command releaseCommand = new ReleaseCommand(id, priority, delay);
        Response response = eventLoop.write(releaseCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("RELEASED");
    }

    public boolean bury(long id, int priority) throws InterruptedException {
        Command buryCommand = new BuryCommand(id, priority);
        Response response = eventLoop.write(buryCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("BURIED");
    }

    public boolean touch(long id) throws InterruptedException {
        Command touchCommand = new TouchCommand(id);
        Response response = eventLoop.write(touchCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("TOUCHED");
    }

    public boolean watch(String tube) throws InterruptedException {
        Command watchCommand = new WatchCommand(tube);
        Response response = eventLoop.write(watchCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    public boolean ignore(String tube) throws InterruptedException {
        Command ignoreCommand = new IgnoreCommand(tube);
        Response response = eventLoop.write(ignoreCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("WATCHING");
    }

    private Job peek(PeekCommand peekCommand) throws InterruptedException {
        Response response = eventLoop.write(peekCommand);
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

    public Job peek(long id) throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(id);
        return peek(peekCommand);
    }

    public Job peekReady() throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.ready);
        return peek(peekCommand);
    }

    public Job peekDelayed() throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.delayed);
        return peek(peekCommand);
    }

    public Job peekBuried() throws InterruptedException {
        PeekCommand peekCommand = new PeekCommand(PeekCommand.PeekType.buried);
        return peek(peekCommand);
    }

    public boolean kick(long bound) throws InterruptedException {
        Command kickCommand = new KickCommand(bound);
        Response response = eventLoop.write(kickCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }

    public boolean kickJob(long id) throws InterruptedException {
        Command kickCommand = new KickJobCommand(id);
        Response response = eventLoop.write(kickCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("KICKED");
    }

    public List<String> listTubes() throws InterruptedException, IOException {
        Command listTubesCommand = new ListTubesCommand();
        Response response = eventLoop.write(listTubesCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public String listTubeUsed() throws InterruptedException {
        Command listTubeUsedCommand = new ListTubeUsedCommand();
        Response response = eventLoop.write(listTubeUsedCommand);
        log.debug("response status {}", response.getStatusLine());
        String[] splits = response.getStatusLine().split(" ");
        return splits[splits.length - 1];
    }

    public List<String> listTubesWatched() throws InterruptedException, IOException {
        Command listTubesWatchedCommand = new ListTubesWatchedCommand();
        Response response = eventLoop.write(listTubesWatchedCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2List(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public boolean pauseTube(String tube, int delay) throws InterruptedException {
        Command pauseTubeCommand = new PauseTubeCommand(tube, delay);
        Response response = eventLoop.write(pauseTubeCommand);
        log.debug("response status {}", response.getStatusLine());
        return response.getStatusLine().startsWith("PAUSED");
    }

    private Map<String, String> stats(StatsCommand statsCommand) throws InterruptedException, IOException {
        Response response = eventLoop.write(statsCommand);
        log.debug("response status {}", response.getStatusLine());
        if (response.getStatusLine().startsWith("OK")) {
            return YamlUtil.yaml2Map(Charset.forName("UTF-8"), response.getData());
        }
        return null;
    }

    public Map<String, String> stats() throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand();
        return stats(statsCommand);
    }

    public Map<String, String> statsJob(long id) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(id);
        return stats(statsCommand);
    }

    public Map<String, String> statsTube(String tube) throws IOException, InterruptedException {
        StatsCommand statsCommand = new StatsCommand(tube);
        return stats(statsCommand);
    }

    public void quit() throws InterruptedException {
        eventLoop.quit();
    }
}
