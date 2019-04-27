package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class Response implements Serializable {

    private Object resObj;

    public void setObj(Object obj) {
        this.resObj = obj;
    }

    public Object getResObj() {
        return resObj;
    }

    @Override
    public String toString() {
        return resObj.toString();
    }
}
