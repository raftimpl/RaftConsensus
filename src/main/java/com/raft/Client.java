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
    public void testSnapshot() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        Peer p = getRandomAddr();
        System.out.println(p);

        Command c1 = new Command("a", "9999", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());


        ClientResp resp = (ClientResp) rpcClient.send(request1);

    }

    @Test
    public void test() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("a", "1111", Command.PUT);
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

        request1.setReqObj(c2);

        ClientResp resp2 = (ClientResp) rpcClient.send(request1);
        System.out.println("c2Result: =" + resp2);

        request1.setReqObj(c3);
        ClientResp resp3 = (ClientResp) rpcClient.send(request1);

        System.out.println("c3Result: =" + resp3);
    }

    @Test
    public void test2() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        Peer p = getRandomAddr();
        System.out.println(p);

        Command c1 = new Command("d", "111", Command.PUT);

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

        Command c1 = new Command("z", "555", Command.PUT);
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
        Command c1 = new Command("h", "6666", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p.getAddr());
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

}

