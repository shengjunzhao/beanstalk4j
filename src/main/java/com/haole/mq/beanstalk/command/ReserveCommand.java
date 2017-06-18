package com.haole.mq.beanstalk.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class ReserveCommand extends AbstractCommand {

    private final static Logger log = LoggerFactory.getLogger(ReserveCommand.class);

    public ReserveCommand(long timeout) {
        if (timeout > 0)
            setCommandLine("reserve-with-timeout " + timeout);
        else
            setCommandLine("reserve");
    }
}
