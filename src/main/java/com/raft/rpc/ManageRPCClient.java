package com.raft.rpc;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.raft.pojo.ClusterRequest;
import com.raft.pojo.Response;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ManageRPCClient {

    private static RpcClient client = new RpcClient();

    static {
        client.init();
    }

    public Response send(ClusterRequest request) {
        Response res = null;
        try {
            res = (Response) client.invokeSync(request.getAddr(), request, 10 * 1000);
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return res;
    }
}
