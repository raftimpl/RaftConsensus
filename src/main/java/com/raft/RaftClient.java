package com.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.raft.pojo.*;
import com.raft.rpc.NodeRPCClient;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 向外界提供接口的类
 * <p>
 * created by Ethan-Walker on 2019/5/14
 */
public class RaftClient {

    private static String getRandom() {
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

    private static NodeRPCClient rpcClient;

    static {
        rpcClient = new NodeRPCClient();
    }

    private RaftClient() {
    }

    public static String getKey(String key) {
        Command c = new Command(key, Command.GET);
        Request<Command> req = new Request<>();
        req.setReqObj(c);
        req.setType(Request.RequestType.CLIENT);
        long current = System.currentTimeMillis();
        long end = current + 1000 * 60; // 60s 还不返回结果，超时
        while (System.currentTimeMillis() < end) {
            String p = getRandom();
            req.setUrl(p);
            Response resp = rpcClient.send(req);
            if (resp != null) {
                return (String) ((ClientResp) resp).getResult();
            }
        }
        return null;
    }

    public static boolean put(String key, String value) {
        Command c = new Command(key, value, Command.PUT);

        Request<Command> req = new Request<>();
        req.setType(Request.RequestType.CLIENT);
        req.setReqObj(c);

        long current = System.currentTimeMillis();
        long end = current + 1000 * 60; // 60s 还不返回结果，超时
        while (System.currentTimeMillis() < end) {
            String p = getRandom();
            req.setUrl(p);
            Response resp = rpcClient.send(req);
            if (resp != null) {
                return (Boolean) ((ClientResp) resp).getResult();
            }
        }
        // 超时
        return false;
    }


    public static boolean addServer(String addr) {
        return true;
    }

    public static boolean removeServer(String addr) {
        return true;
    }
}
