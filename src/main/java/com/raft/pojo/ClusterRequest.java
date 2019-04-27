package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ClusterRequest<T> implements Serializable {

    private RequestType requestType;

    private T reqObj;

    private String addr; // 该请求发送的目标地址

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getAddr() {
        return addr;
    }

    public void setReqObj(T reqObj) {
        this.reqObj = reqObj;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public T getReqObj() {
        return reqObj;
    }

    public enum RequestType {
        GET_ALL_NODE,
        GET_RANDOM_NODE,
        REGISTER_NODE,
        SIGN
    }
}
