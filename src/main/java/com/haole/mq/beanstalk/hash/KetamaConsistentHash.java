package com.haole.mq.beanstalk.hash;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * 一致性hash
 * Created by shengjunzhao on 2017/6/6.
 */
public class KetamaConsistentHash {

    private volatile TreeMap<Long, String> ketamaNodes = new TreeMap<>();
    private volatile Set<String> allServers;

    private final int numReps = 160;


    public KetamaConsistentHash(Set<String> servers) {
        this.allServers = servers;
        setKetamaNodes(servers);
    }

    public String getServerforKey(long hash) {
        final String server;
        if (!ketamaNodes.containsKey(hash)) {
            Long h = ketamaNodes.ceilingKey(hash);
            if (null == h)
                h = ketamaNodes.firstKey();
            hash = h.longValue();
        }
        server = ketamaNodes.get(hash);
        return server;
    }

    public void addServer(String server) {
        int oldLen = this.allServers.size();
        allServers.add(server);
        int len = this.allServers.size();
        if (oldLen != len)
            setKetamaNodes(allServers);
    }

    public void addServers(Set<String> servers) {
        int oldLen=this.allServers.size();
        for (String server:servers)
            this.allServers.add(server);
        int len = this.allServers.size();
        if (oldLen!=len)
            setKetamaNodes(this.allServers);
    }

    public void removeServer(String server) {
        int oldLen = this.allServers.size();
        allServers.remove(server);
        int len = this.allServers.size();
        if (oldLen != len)
            setKetamaNodes(allServers);
    }


    protected void setKetamaNodes(Set<String> servers) {
        int nodeCount = servers.size();
        for (String server : servers) {
            for (int i = 0; i < numReps / 4; i++) {
                for (long position : ketamaNodePositionsAtIteration(server, i)) {
                    ketamaNodes.put(position, server);
                }
            }
        }
    }

    private List<Long> ketamaNodePositionsAtIteration(String server, int iteration) {
        List<Long> positions = new ArrayList<>();
        byte[] digest = KetamaHash.computeMd5(server + "-" + iteration);
        for (int h = 0; h < 4; h++) {
            Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                    | (digest[h * 4] & 0xFF);
            positions.add(k);
        }
        return positions;
    }


}
