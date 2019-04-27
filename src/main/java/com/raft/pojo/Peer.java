package com.raft.pojo;

import java.io.Serializable;
import java.util.Objects;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class Peer implements Serializable {
    private String addr; // ip:port
    private int port;

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public Peer(String addr) {
        this.addr = addr;
    }

    public Peer(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    @Override
    public String toString() {
        return addr;
    }

    @Override
    public boolean equals(Object obj) {
        Peer p = (Peer) obj;
        return addr.equals(p.getAddr());
    }

    @Override
    public int hashCode() {
        return Objects.hash(addr);
    }
}
