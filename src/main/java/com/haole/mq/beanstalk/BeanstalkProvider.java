package com.haole.mq.beanstalk;

/**
 * Created by shengjunzhao on 2017/6/7.
 */
public interface BeanstalkProvider extends BeanstalkClient {
    boolean use(String tube);

    long put(int priority, int delay, int ttr, byte[] data);

}
