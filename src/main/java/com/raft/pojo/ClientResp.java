package com.raft.pojo;

/**
 * created by Ethan-Walker on 2019/4/13
 */
public class ClientResp extends Response {

    private Object result;

    public static ClientResp yes(Object obj) {
        ClientResp resp = new ClientResp();
        resp.result = obj;
        return resp;
    }

    public static ClientResp no(Object obj) {
        ClientResp resp = new ClientResp();
        resp.result = obj;
        return resp;
    }

    public Object getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ClientResp{" +
                "result=" + result +
                '}';
    }
}
