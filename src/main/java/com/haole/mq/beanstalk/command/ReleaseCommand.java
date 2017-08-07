package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class ReleaseCommand extends AbstractCommand {

    public ReleaseCommand(long id, int priority, int delay) {
        setCommandLine("release " + id + " " + priority + " " + delay);
    }
}
