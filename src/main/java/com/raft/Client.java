package com.raft;

import com.raft.pojo.*;
import com.raft.rpc.ManageRPCClient;
import com.raft.rpc.NodeRPCClient;
import org.junit.Test;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class Client {


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
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("a", "111", Command.PUT);
        Command c2 = new Command("b", "222", Command.PUT);
        Command c3 = new Command("c", "333", Command.PUT);

        Peer p = getRandomAddr();
        System.out.println(p);

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);

        Request request2 = new Request();
        request2.setType(Request.RequestType.CLIENT);
        request2.setDesc("发送put请求");
        request2.setUrl(p.getAddr());
        request2.setReqObj(c2);
        ClientResp resp2 = (ClientResp) rpcClient.send(request2);
        System.out.println("c2Result: =" + resp2);

        Request request3 = new Request();
        request3.setType(Request.RequestType.CLIENT);
        request3.setDesc("发送put请求");
        request3.setUrl(p.getAddr());
        request3.setReqObj(c3);
        ClientResp resp3 = (ClientResp) rpcClient.send(request3);
        System.out.println("c3Result: =" + resp3);
    }

    @Test
    public void test2() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        Peer p = getRandomAddr();
        System.out.println(p);

        Command c1 = new Command("d", "444", Command.PUT);

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

    @Test
    public void test3() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        Peer p = getRandomAddr();
        System.out.println(p);

        Command c1 = new Command("e", "555", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);


    }

    @Test
    public void test4() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        Peer p = getRandomAddr();
        System.out.println(p);

        Command c1 = new Command("a", "666", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

}

