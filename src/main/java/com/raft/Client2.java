package com.raft;

import org.junit.Test;

/**
 * created by Ethan-Walker on 2019/4/14
 */
public class Client2 {

    /**
     * Put 方法测试
     */
    @Test
    public void test() {
        System.out.println(RaftClient.put("a", "111"));
        System.out.println(RaftClient.put("b", "222"));
        System.out.println(RaftClient.put("c", "333"));
        System.out.println(RaftClient.put("d", "444"));
        System.out.println(RaftClient.put("e", "555"));
    }

    @Test
    public void test2() {
        System.out.println(RaftClient.put("z", "111111"));
    }

    /**
     * Get 方法测试
     */
    @Test
    public void test3() {
        System.out.println(RaftClient.getKey("a"));
        System.out.println(RaftClient.getKey("b"));
        System.out.println(RaftClient.getKey("c"));
        System.out.println(RaftClient.getKey("d"));
        System.out.println(RaftClient.getKey("z"));
    }

}
