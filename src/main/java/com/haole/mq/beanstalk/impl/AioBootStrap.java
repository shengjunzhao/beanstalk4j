package com.haole.mq.beanstalk.aio.channel;

import com.haole.mq.beanstalk.hash.KetamaConsistentHash;
import com.haole.mq.beanstalk.hash.KetamaHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private KetamaConsistentHash consistentHash;

    /**
     * 服务器列表，格式：ip:port
     */
    private Set<String> allServers = new CopyOnWriteArraySet();
    private ConcurrentHashMap<String, AioSocketChannelEventLoop> tubePool = new ConcurrentHashMap<>();
    private Lock lock = new ReentrantLock();

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

    public AioSocketChannelEventLoop getChannel(String tube, boolean isProvider) throws InterruptedException {
        try {
            lock.lock();
            String ptube = isProvider ? tube + "_p" : tube + "_c";
            AioSocketChannelEventLoop channel = tubePool.get(ptube);
            if (null == channel) {
                String server = getServersOfTube(tube);
                String[] splits = server.split(":");
                String host = splits[0];
                int port = Integer.valueOf(splits[1]).intValue();
//                channel = connect(host, port);
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


}
