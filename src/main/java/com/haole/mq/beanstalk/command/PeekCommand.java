package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class PeekCommand extends AbstractCommand {

    public PeekCommand(long id) {
        setCommandLine("peek " + id);
    }

    public PeekCommand(PeekType pt) {
        setCommandLine("peek-" + pt.name());
    }

    public enum PeekType {ready, delayed, buried}
}
