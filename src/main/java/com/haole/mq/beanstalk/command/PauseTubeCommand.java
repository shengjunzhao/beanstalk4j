package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class PauseTubeCommand extends AbstractCommand {

    public PauseTubeCommand(String tube, int delay) {
        setCommandLine("pause-tube " + tube + " " + delay);
    }
}
