package com.raft.rpc;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import com.raft.NodeServer;
import com.raft.pojo.*;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class RPCServer {
    private static RpcServer server;

    private NodeServer node;

    public RPCServer(int port, NodeServer node) {
        this.node = node;
        server = new RpcServer(port);
        server.registerUserProcessor(new MUserProcessor<Request>() {
            public Object handleRequest(BizContext bizContext, Request request) throws Exception {
                return handlerRequest(request);
            }
        });
    }

    public Response handlerRequest(Request request) {
        if (request.getType() == Request.RequestType.VOTE) {
            return node.handleRequestVote((VoteParam) request.getReqObj());
        } else if (request.getType() == Request.RequestType.HEARTBEAT) {
            return node.handleHeartbeat((AppendEntryParam) request.getReqObj());
        } else if (request.getType() == Request.RequestType.CLIENT) {
            return node.handleClientRequest(request);
        } else if (request.getType() == Request.RequestType.APPEND_ENTRY) {
            return node.handleAppendEntry((AppendEntryParam) request.getReqObj());
        } else if (request.getType() == Request.RequestType.QUERY_ENTRY) {
            return node.handleGetRequest(request);
        }
        return null;
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop();
    }
}
