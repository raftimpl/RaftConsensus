package com.raft.rpc;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.raft.pojo.Request;
import com.raft.pojo.Response;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class RPCClient {


    private static RpcClient client = new RpcClient();

    static {
        client.init();
    }

    public Response send(Request request) {
        Response resp = null;
        try {
            resp = (Response) client.invokeSync(request.getUrl(), request, 20 * 1000);
        } catch (RemotingException e) {
            e.printStackTrace();
            System.out.println(request.getUrl() + " 连接失败");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return resp;
    }
}
