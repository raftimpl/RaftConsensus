package com.raft.rpc;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import com.raft.ManageServer;
import com.raft.pojo.ClusterRequest;
import com.raft.pojo.Peer;
import com.raft.pojo.Response;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ManageRPCServer {

    private static RpcServer server;

    private ManageServer node;

    public ManageRPCServer(int port, ManageServer node) {
        this.node = node;
        server = new RpcServer(port);
        server.registerUserProcessor(new ClusterUserProcessor<ClusterRequest>() {
            public Object handleRequest(BizContext bizContext, ClusterRequest request) {
                return handleClusterRequest(request);
            }
        });
    }

    public Response handleClusterRequest(ClusterRequest request) {
        if (request.getRequestType() == ClusterRequest.RequestType.SIGN) {
            return node.sign((Peer) request.getReqObj());
        } else if (request.getRequestType() == ClusterRequest.RequestType.GET_ALL_NODE) {
            return node.getAllPeers();
        } else if (request.getRequestType() == ClusterRequest.RequestType.GET_RANDOM_NODE) {
            return node.getRandomPeer();
        } else if (request.getRequestType() == ClusterRequest.RequestType.REGISTER_NODE) {
            return node.registerPeer((Peer) request.getReqObj());
        }
        return null;
    }

    public void start() {
        server.start();
    }
}
