package com.haole.mq.beanstalk.command;

/**
 * Created by shengjunzhao on 2017/5/27.
 */
public class Response {
    private String statusLine;
    private byte[] data;

    public void reset() {
        this.statusLine = "";
        data = new byte[0];
    }

    public Response clone() {
        Response response = new Response();
        response.setStatusLine(this.statusLine);
        byte[] cloneData = new byte[this.data.length];
        System.arraycopy(this.data,0,cloneData,0,this.data.length);
        response.setData(cloneData);
        return response;
    }

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
