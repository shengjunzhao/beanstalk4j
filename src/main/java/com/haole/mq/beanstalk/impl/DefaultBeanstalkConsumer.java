package com.haole.mq.beanstalk.impl;

import com.haole.mq.beanstalk.BeanstalkClient;
import com.haole.mq.beanstalk.BeanstalkConsumer;
import com.haole.mq.beanstalk.Job;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * beanstalk client 消费端
 * Created by shengjunzhao on 2017/6/8.
 */
public class DefaultBeanstalkConsumer implements BeanstalkClient, BeanstalkConsumer {

    private final static Logger log = LoggerFactory.getLogger(DefaultBeanstalkProvider.class);

    private DefaultBeanstalkClient client;
    private Channel channel;

    public DefaultBeanstalkConsumer(Set<String> servers, String tube) throws InterruptedException {
        client = DefaultBeanstalkClient.getInstance();
        client.addServers(servers);
        Channel ch = client.getChannel(tube, false);
        if (null == ch) {
            throw new RuntimeException("can't connect server");
        }
        this.channel = ch;
        if (!client.watch(ch, tube)) {
            throw new RuntimeException("can't watch tube " + tube);
        }
    }

    @Override
    public boolean watch(String tube) {
        try {
            boolean result = client.watch(channel, tube);
            return result;
        } catch (InterruptedException e) {
            log.error("watch exception {}", e);
        }
        return false;
    }

    @Override
    public Job reserve(long timeout) {
        try {
            Job job = client.reserve(channel, timeout);
            return job;
        } catch (InterruptedException e) {
            log.error("reserve exception {}", e);
        }
        return null;
    }

    @Override
    public boolean delete(long id) {
        try {
            boolean result = client.delete(channel, id);
            return result;
        } catch (InterruptedException e) {
            log.error("delete exception {}", e);
        }
        return false;
    }

    @Override
    public boolean release(long id, int priority, int delay) {
        try {
            boolean result = client.release(channel, id, priority, delay);
            return result;
        } catch (InterruptedException e) {
            log.error("release exception {}", e);
        }
        return false;
    }

    @Override
    public boolean bury(long id, int priority) {
        try {
            boolean result = client.bury(channel, id, priority);
            return result;
        } catch (InterruptedException e) {
            log.error("bury exception {}", e);
        }
        return false;
    }

    @Override
    public boolean touch(long id) {
        try {
            boolean result = client.touch(channel, id);
            return result;
        } catch (InterruptedException e) {
            log.error("touch exception {}", e);
        }
        return false;
    }

    @Override
    public boolean ignore(String tube) {
        try {
            boolean result = client.ignore(channel, tube);
            return result;
        } catch (InterruptedException e) {
            log.error("ignore exception {}", e);
        }
        return false;
    }

    @Override
    public Job peek(long id) {
        try {
            Job job = client.peek(channel, id);
            return job;
        } catch (InterruptedException e) {
            log.error("peek exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekReady() {
        try {
            Job job = client.peekReady(channel);
            return job;
        } catch (InterruptedException e) {
            log.error("peekReady exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekDelayed() {
        try {
            Job job = client.peekDelayed(channel);
            return job;
        } catch (InterruptedException e) {
            log.error("peekDelayed exception {}", e);
        }
        return null;
    }

    @Override
    public Job peekBuried() {
        try {
            Job job = client.peekBuried(channel);
            return job;
        } catch (InterruptedException e) {
            log.error("peekBuried exception {}", e);
        }
        return null;
    }

    @Override
    public boolean kick(long bound) {
        try {
            boolean result = client.kick(channel, bound);
            return result;
        } catch (InterruptedException e) {
            log.error("kick exception {}", e);
        }
        return false;
    }

    @Override
    public boolean kickJob(long id) {
        try {
            boolean result = client.kickJob(channel, id);
            return result;
        } catch (InterruptedException e) {
            log.error("kick exception {}", e);
        }
        return false;
    }

    @Override
    public List<String> listTubes() {
        try {
            List<String> tubes = client.listTubes(channel);
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
            String tube = client.listTubeUsed(channel);
            return tube;
        } catch (InterruptedException e) {
            log.error("listTubeUsed exception {}", e);
        }
        return null;
    }

    @Override
    public List<String> listTubesWatched() {
        try {
            List<String> watchedTubes = client.listTubesWatched(channel);
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
            boolean result = client.pauseTube(channel, tube, delay);
            return result;
        } catch (InterruptedException e) {
            log.error("pauseTube exception {}", e);
        }
        return false;
    }

    @Override
    public Map<String, String> stats() {
        try {
            Map<String, String> stat = client.stats(channel);
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
            Map<String, String> stat = client.statsJob(channel, id);
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
            Map<String, String> stat = client.statsTube(channel, tube);
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
            client.quit(channel);
        } catch (InterruptedException e) {
            log.error("quit exception {}", e);
        }
    }
}
