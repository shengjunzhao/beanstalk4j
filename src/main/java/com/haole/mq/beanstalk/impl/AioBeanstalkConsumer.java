package com.haole.mq.beanstalk.impl;

import com.haole.mq.beanstalk.BeanstalkConsumer;
import com.haole.mq.beanstalk.Job;
import com.haole.mq.beanstalk.aio.channel.AioSocketChannelEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by shengjunzhao on 2017/8/5.
 */
public class AioBeanstalkConsumer implements BeanstalkConsumer {

    private static final Logger log = LoggerFactory.getLogger(AioBeanstalkConsumer.class);

    private AioBootStrap client;
    private AioSocketChannelEventLoop eventLoop;

    public AioBeanstalkConsumer(Set<String> servers, String tube) throws IOException, InterruptedException {
        client = AioBootStrap.getInstance();
        client.addServers(servers);
        AioSocketChannelEventLoop channelEventLoop = client.getChannel(tube, false);
        if (null == channelEventLoop) {
            throw new RuntimeException("can't connect server");
        }
        this.eventLoop = channelEventLoop;
        if (!client.watch(eventLoop, tube)) {
            throw new RuntimeException("can't use tube " + tube);
        }
    }

    @Override
    public boolean watch(String tube) {
        try {
            boolean result = client.watch(eventLoop, tube);
            return result;
        } catch (InterruptedException e) {
            log.error("watch exception {}", e);
        }
        return false;
    }

    @Override
    public Job reserve(long timeout) {
        try {
            Job job = client.reserve(eventLoop, timeout);
            return job;
        } catch (InterruptedException e) {
            log.error("reserve exception {}", e);
        }
        return null;
    }

    @Override
    public boolean delete(long id) {
        try {
            boolean result = client.delete(eventLoop, id);
            return result;
        } catch (InterruptedException e) {
            log.error("delete exception {}", e);
        }
        return false;
    }

    @Override
    public boolean release(long id, int priority, int delay) {
        try {
            boolean result = client.release(eventLoop, id, priority, delay);
            return result;
        } catch (InterruptedException e) {
            log.error("release exception {}", e);
        }
        return false;
    }

    @Override
    public boolean bury(long id, int priority) {
        try {
            boolean result = client.bury(eventLoop, id, priority);
            return result;
        } catch (InterruptedException e) {
            log.error("bury exception {}", e);
        }
        return false;
    }

    @Override
    public boolean touch(long id) {
        try {
            boolean result = client.touch(eventLoop, id);
            return result;
        } catch (InterruptedException e) {
            log.error("touch exception {}", e);
        }
        return false;
    }

    @Override
    public boolean ignore(String tube) {
        try {
            boolean result = client.ignore(eventLoop, tube);
            return result;
        } catch (InterruptedException e) {
            log.error("ignore exception {}", e);
        }
        return false;
    }

    @Override
    public Job peek(long id) {
        try {
            Job job = client.peek(eventLoop, id);
            return job;
        } catch (InterruptedException e) {
            log.error("peek exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekReady() {
        try {
            Job job = client.peekReady(eventLoop);
            return job;
        } catch (InterruptedException e) {
            log.error("peekReady exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekDelayed() {
        try {
            Job job = client.peekDelayed(eventLoop);
            return job;
        } catch (InterruptedException e) {
            log.error("peekDelayed exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekBuried() {
        try {
            Job job = client.peekBuried(eventLoop);
            return job;
        } catch (InterruptedException e) {
            log.error("peekBuried exception {}", e);
        }
        return null;
    }

    @Override
    public boolean kick(long bound) {
        try {
            boolean result = client.kick(eventLoop, bound);
            return result;
        } catch (InterruptedException e) {
            log.error("kick exception {}", e);
        }
        return false;
    }

    @Override
    public boolean kickJob(long id) {
        try {
            boolean result = client.kickJob(eventLoop, id);
            return result;
        } catch (InterruptedException e) {
            log.error("kick exception {}", e);
        }
        return false;
    }

    @Override
    public List<String> listTubes() {
        try {
            List<String> tubes = client.listTubes(eventLoop);
            return tubes;
        } catch (InterruptedException e) {
            log.error("listTubes exception {}", e);
        } catch (IOException e) {
            log.error("listTubes exception {}", e);
        }
        return null;
    }

    @Override
    public String listTubeUsed() {
        try {
            String tube = client.listTubeUsed(eventLoop);
            return tube;
        } catch (InterruptedException e) {
            log.error("listTubeUsed exception {}", e);
        }
        return null;
    }

    @Override
    public List<String> listTubesWatched() {
        try {
            List<String> watchedTubes = client.listTubesWatched(eventLoop);
            return watchedTubes;
        } catch (InterruptedException e) {
            log.error("listTubesWatched exception {}", e);
        } catch (IOException e) {
            log.error("listTubesWatched exception {}", e);
        }
        return null;
    }

    @Override
    public boolean pauseTube(String tube, int delay) {
        try {
            boolean result = client.pauseTube(eventLoop, tube, delay);
            return result;
        } catch (InterruptedException e) {
            log.error("pauseTube exception {}", e);
        }
        return false;
    }

    @Override
    public Map<String, String> stats() {
        try {
            Map<String, String> stat = client.stats(eventLoop);
            return stat;
        } catch (IOException e) {
            log.error("stats exception {}", e);
        } catch (InterruptedException e) {
            log.error("stats exception {}", e);
        }
        return null;
    }

    @Override
    public Map<String, String> statsJob(long id) {
        try {
            Map<String, String> stat = client.statsJob(eventLoop, id);
            return stat;
        } catch (IOException e) {
            log.error("statsJob exception {}", e);
        } catch (InterruptedException e) {
            log.error("statsJob exception {}", e);
        }
        return null;
    }

    @Override
    public Map<String, String> statsTube(String tube) {
        try {
            Map<String, String> stat = client.statsTube(eventLoop, tube);
            return stat;
        } catch (IOException e) {
            log.error("statsTube exception {}", e);
        } catch (InterruptedException e) {
            log.error("statsTube exception {}", e);
        }
        return null;
    }

    @Override
    public void quit() {
        try {
            client.quit(eventLoop);
        } catch (InterruptedException e) {
            log.error("quit exception {}", e);
        }
    }
}
