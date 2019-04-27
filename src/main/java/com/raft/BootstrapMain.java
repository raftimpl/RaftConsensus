package com.raft;

import org.junit.Test;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class BootstrapMain {


    public static void main(String[] args) {

        String managerServer = "localhost:8889";

        String curNodePort = System.getProperty("serverPort");
        int port = Integer.parseInt(curNodePort);

        NodeServer server = new NodeServer();

        server.init("localhost:" + port, port, managerServer);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.destroy();
            }
        });



    }
}
