package com.raft;

import com.raft.pojo.*;
import com.raft.rpc.ManageRPCClient;
import com.raft.rpc.NodeRPCClient;
import org.junit.Test;

/**
 * created by Ethan-Walker on 2019/4/14
 */
public class Client2 {

    public Peer getRandomAddr() {
        ManageRPCClient client = new ManageRPCClient();
        String manageServer = "localhost:8889";

        ClusterRequest request = new ClusterRequest();
        request.setAddr(manageServer);
        request.setRequestType(ClusterRequest.RequestType.GET_RANDOM_NODE);

        Response res = client.send(request);
        Object obj = res.getResObj();
        Peer p = null;
        if (obj != null) {
            p = (Peer) obj;
        }
        return p;
    }

    @Test
    public void test() {

        Peer p = getRandomAddr();
        NodeRPCClient client = new NodeRPCClient();
        Request<Command> request = new Request<>();
        Command c1 = new Command("a", Command.GET);
        Command c2 = new Command("b", Command.GET);
        Command c3 = new Command("c", Command.GET);

        String addr = p.getAddr();

        request.setType(Request.RequestType.CLIENT);
        request.setUrl(addr);

        request.setReqObj(c1);
        ClientResp result1 = (ClientResp) client.send(request);
        System.out.println("查询 a 得到结果: " + result1.getResult());

        request.setReqObj(c2);
        ClientResp result2 = (ClientResp) client.send(request);
        System.out.println("查询 b 得到结果: " + result2.getResult());

        request.setReqObj(c3);
        ClientResp result3 = (ClientResp) client.send(request);
        System.out.println("查询 c 得到结果: " + result3.getResult());

    }

}
