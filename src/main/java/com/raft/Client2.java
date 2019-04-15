package com.raft;

import com.raft.pojo.ClientResp;
import com.raft.pojo.Command;
import com.raft.pojo.Request;
import com.raft.rpc.RPCClient;

/**
 * created by Ethan-Walker on 2019/4/14
 */
public class Client2 {
    public static void main(String[] args) {
        RPCClient client = new RPCClient();
        Request<Command> request = new Request<>();
        Command c1 = new Command("a", Command.GET);
        Command c2 = new Command("b", Command.GET);
        Command c3 = new Command("c", Command.GET);

        String addr = "localhost:8003";

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
