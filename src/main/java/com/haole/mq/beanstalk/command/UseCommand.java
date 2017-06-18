package com.haole.mq.beanstalk.command;

/**
 * use tube
 * Created by shengjunzhao on 2017/5/27.
 */
public class UseCommand extends AbstractCommand {

    public UseCommand(String tube) {
        setCommandLine("use " + tube);
    }
}
