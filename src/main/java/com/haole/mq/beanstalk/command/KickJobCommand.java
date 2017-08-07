package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class KickJobCommand extends AbstractCommand {

    public KickJobCommand(long id) {
        setCommandLine("kick-job " + id);
    }
}
