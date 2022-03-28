package com.haole.mq.beanstalk.command;


/**
 * 允许一个worker请求在一个job获取更多执行的时间
 * 在收到DEADLINE_SOON是可以发生给命令
 * Created by shengjunzhao on 2017/5/29.
 */
public class TouchCommand extends AbstractCommand {
    public TouchCommand(long id) {
        setCommandLine("touch " + id);
    }
}
