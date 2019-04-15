package com.raft;

import java.util.Arrays;
import java.util.List;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class BootstrapMain {
    public static void main(String[] args) {

        List<String> ipAddrs = Arrays.asList("localhost:8001", "localhost:8002", "localhost:8003", "localhost:8004", "localhost:8005");
        String curServerPort = System.getProperty("serverPort");

        NodeServer server = new NodeServer();
        server.init(ipAddrs, Integer.parseInt(curServerPort), "localhost:" + curServerPort);

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                server.destroy();
            }
        });
    }
}
