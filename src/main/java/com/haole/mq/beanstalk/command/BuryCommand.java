package com.haole.mq.beanstalk.command;

/**
 * 休眠 将一个job放入休眠队列
 * Created by shengjunzhao on 2017/5/29.
 */
public class BuryCommand extends AbstractCommand {

    public BuryCommand(long id, int priority) {
        setCommandLine("bury " + id + " " + priority);
    }
}
