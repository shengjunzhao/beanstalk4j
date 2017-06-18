package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class IgnoreCommand extends AbstractCommand {
    public IgnoreCommand(String tube) {
        setCommandLine("ignore " + tube);
    }
}
