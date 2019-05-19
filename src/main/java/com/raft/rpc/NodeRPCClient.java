package com.raft.rpc;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.raft.pojo.Request;
import com.raft.pojo.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class NodeRPCClient {
    public static RpcClient client = new RpcClient();
    private static final Logger LOG = LoggerFactory.getLogger(NodeRPCClient.class);


    static {
        client.init();
    }

    public Response send(Request request) {
        Response resp = null;
        try {
            resp = (Response) client.invokeSync(request.getUrl(), request, 5 * 1000);
        } catch (RemotingException e) {
            LOG.warn("向 " + request.getUrl() + " " + request.getDesc() + " 连接超时");
        } catch (InterruptedException e) {
//            e.printStackTrace();
        }
        return resp;
    }

}
