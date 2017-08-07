package com.haole.mq.beanstalk;

/**
 * Reserve 返回的job
 * Created by shengjunzhao on 2017/5/27.
 */
public class Job {
    private long id;
    private byte[] data;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
