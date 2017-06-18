package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class DeleteCommand extends AbstractCommand {

    public DeleteCommand(long id) {
        setCommandLine("delete " + id);
    }
}
