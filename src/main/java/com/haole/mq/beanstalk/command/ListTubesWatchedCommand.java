package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class ListTubesWatchedCommand extends AbstractCommand{

    public ListTubesWatchedCommand() {
        setCommandLine("list-tubes-watched");
    }
}
