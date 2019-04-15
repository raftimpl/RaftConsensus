package com.raft;

import com.raft.pojo.ClientResp;
import com.raft.pojo.Command;
import com.raft.pojo.Request;
import com.raft.rpc.RPCClient;
import org.junit.Test;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class Client {


    @Test
    public void test() {
        RPCClient rpcClient = new RPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("a", "111", Command.PUT);
        Command c2 = new Command("b", "222", Command.PUT);
        Command c3 = new Command("c", "333", Command.PUT);

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl("localhost:8002");
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);

        Request request2 = new Request();
        request2.setType(Request.RequestType.CLIENT);
        request2.setDesc("发送put请求");
        request2.setUrl("localhost:8002");
        request2.setReqObj(c2);
        ClientResp resp2 = (ClientResp) rpcClient.send(request2);
        System.out.println("c2Result: =" + resp2);

        Request request3 = new Request();
        request3.setType(Request.RequestType.CLIENT);
        request3.setDesc("发送put请求");
        request3.setUrl("localhost:8002");
        request3.setReqObj(c3);
        ClientResp resp3 = (ClientResp) rpcClient.send(request3);
        System.out.println("c3Result: =" + resp3);
    }

    @Test
    public void test2() {
        RPCClient rpcClient = new RPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("d", "444", Command.PUT);

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl("localhost:8002");
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

    @Test
    public void test3() {
        RPCClient rpcClient = new RPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("e", "555", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl("localhost:8002");
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);


    }

    @Test
    public void test4() {
        RPCClient rpcClient = new RPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("f", "666", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl("localhost:8001");
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);

    }

}

