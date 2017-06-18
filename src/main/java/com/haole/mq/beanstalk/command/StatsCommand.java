package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class StatsCommand extends AbstractCommand {

    public StatsCommand() {
        setCommandLine("stats");
    }

    public StatsCommand(long id) {
        setCommandLine("stats-job " + id);
    }

    public StatsCommand(String tube) {
        setCommandLine("stats-tube " + tube);
    }
}
