package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class QuitCommand extends AbstractCommand {
    public QuitCommand() {
        setCommandLine("quit");
    }

}
