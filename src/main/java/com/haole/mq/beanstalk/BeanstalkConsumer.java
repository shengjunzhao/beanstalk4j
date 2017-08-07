package com.haole.mq.beanstalk;


/**
 * Created by shengjunzhao on 2017/6/7.
 */
public interface BeanstalkConsumer extends BeanstalkClient {

    boolean watch(String tube);
    Job reserve(long timeout);
    boolean delete(long id);
    boolean release(long id, int priority, int delay);
    boolean bury(long id, int priority);
    boolean touch(long id);
    boolean ignore(String tube);


}
