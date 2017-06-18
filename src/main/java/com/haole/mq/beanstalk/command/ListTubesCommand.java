package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class ListTubesCommand extends AbstractCommand {

    public ListTubesCommand() {
        setCommandLine("list-tubes");
    }
}
