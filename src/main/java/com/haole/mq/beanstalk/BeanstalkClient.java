package com.haole.mq.beanstalk;


import java.util.List;
import java.util.Map;

/**
 * Created by shengjunzhao on 2017/6/7.
 */
public interface BeanstalkClient {

    Job peek(long id);

    Job peekReady();

    Job peekDelayed();

    Job peekBuried();

    boolean kick(long bound);

    boolean kickJob(long id);

    List<String> listTubes();

    String listTubeUsed();

    List<String> listTubesWatched();

    boolean pauseTube(String tube, int delay);

    Map<String, String> stats();

    Map<String, String> statsJob(long id);

    Map<String, String> statsTube(String tube);

    void quit();
}
