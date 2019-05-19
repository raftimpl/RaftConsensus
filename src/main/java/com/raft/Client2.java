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
    public void test1() {
        System.out.println(RaftClient.put("a", "12345"));

    }@Test
    public void test() {
        System.out.println(RaftClient.put("a", "111"));
        System.out.println(RaftClient.put("b", "222"));
        System.out.println(RaftClient.put("c", "333"));

    }

    @Test
    public void testA() {
        System.out.println(RaftClient.put("a", "aaa"));
        System.out.println(RaftClient.put("b", "bbb"));
        System.out.println(RaftClient.put("c", "ccc"));
    }

    @Test
    public void test2() {
        System.out.println(RaftClient.put("d", "444"));
        System.out.println(RaftClient.put("e", "555"));
    }

    @Test
    public void test2A() {
        System.out.println(RaftClient.put("d", "ddd"));
        System.out.println(RaftClient.put("e", "eee"));
    }

    @Test
    public void test3() {
        System.out.println(RaftClient.put("f", "666"));
    }

    @Test
    public void test3A() {
        System.out.println(RaftClient.put("f", "fff"));
        System.out.println(RaftClient.put("g", "ggg"));
    }

    @Test
    public void test4() {
        System.out.println(RaftClient.put("h", "213"));
    }

    /**
     * Get 方法测试
     */
    @Test
    public void test11() {
        System.out.println(RaftClient.getKey("a"));
        System.out.println(RaftClient.getKey("b"));
        System.out.println(RaftClient.getKey("c"));
        System.out.println(RaftClient.getKey("d"));
        System.out.println(RaftClient.getKey("z"));
    }

}
