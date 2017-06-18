package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public class Response {
    private String statusLine;
    private byte[] data;

    public String getStatusLine() {
        return statusLine;
    }

    public void setStatusLine(String statusLine) {
        this.statusLine = statusLine;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
