package com.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class Client {
    static Logger logger1 = LoggerFactory.getLogger(Client.class);
    public static void main(String[] args) {
        logger1.trace("111");
        logger1.debug("222");
        logger1.info("333");
        logger1.warn("444");
        logger1.error("555");
    }
 /*   public String getRandom() {
        List<Peer> otherPeers = new ArrayList<>();
        String addr = null;
        try {
            String s = FileUtils.readFileToString(new File("./cluster.json"));
            JSONObject jsonObject = JSON.parseObject(s);
            JSONArray servers = jsonObject.getJSONArray("servers");

            int index = new Random().nextInt(servers.size());
            addr = servers.getJSONObject(index).getString("addr");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return addr;
    }

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
        String p = getRandom();
        System.out.println(p);

        Command c1 = new Command("a", "9999", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p);


        ClientResp resp = (ClientResp) rpcClient.send(request1);

    }


    @Test
    public void test() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("a", "aaa", Command.PUT);
        Command c2 = new Command("b", "bbb", Command.PUT);
        Command c3 = new Command("c", "ccc", Command.PUT);

        String p = getRandom();
        System.out.println(p);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p);
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
        String p = getRandom();
        System.out.println(p);

        Command c1 = new Command("d", "111", Command.PUT);

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p);
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

    @Test
    public void test3() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        String p = getRandom();
        System.out.println(p);

        Command c1 = new Command("z", "zzz", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p);
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }

    @Test
    public void test4() {
        NodeRPCClient rpcClient = new NodeRPCClient();
        Request<Command> request1 = new Request<>();
        String p = getRandom();
        System.out.println(p);
        Command c1 = new Command("h", "6666", Command.PUT);
        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setDesc("发送put请求");
        request1.setUrl(p);
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp);
    }
*/
}

