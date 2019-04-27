package com.raft;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ManageMain {
    public static void main(String[] args) {
        ManageServer server = new ManageServer(8889);
        server.init();
    }
}
