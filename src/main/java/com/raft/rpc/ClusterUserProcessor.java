package com.raft.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;
import com.raft.pojo.ClusterRequest;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ClusterUserProcessor<T> extends AbstractUserProcessor<T> {
    @Override
    public void handleRequest(BizContext bizContext, AsyncContext asyncContext, T t) {

    }

    @Override
    public Object handleRequest(BizContext bizContext, T t) throws Exception {
        return null;
    }

    @Override
    public String interest() {
        return ClusterRequest.class.getName();
    }
}
