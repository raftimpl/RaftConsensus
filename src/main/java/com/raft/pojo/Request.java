package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class Request<T> implements Serializable {

    private RequestType type;
    private T reqObj;
    private String desc;

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    private String url; // 请求发送给哪个节点  ip:port

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public T getReqObj() {
        return reqObj;
    }

    public void setReqObj(T reqObj) {
        this.reqObj = reqObj;
    }

    public void setType(RequestType type) {
        this.type = type;
    }

    public RequestType getType() {
        return type;
    }

    public enum RequestType {
        VOTE,
        APPEND_ENTRY,
        HEARTBEAT,
        CLIENT, // 客户端发送来的请求
        QUERY_ENTRY
    }
}
