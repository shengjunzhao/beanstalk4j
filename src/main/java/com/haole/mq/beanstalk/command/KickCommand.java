package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class KickCommand extends AbstractCommand {

    public KickCommand(long bound) {
        setCommandLine("kick " + bound);
    }
}
