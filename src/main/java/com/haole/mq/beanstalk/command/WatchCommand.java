package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class WatchCommand extends AbstractCommand {
    public WatchCommand(String tube) {
        setCommandLine("watch " + tube);
    }
}
