package com.raft.pojo;

import java.util.ArrayList;
import java.util.List;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class PeerSet {
    private List<Peer> set;
    private Peer leader;
    private Peer self;
    private List<Peer> otherPeers;


    public PeerSet() {
    }

    public List<Peer> getOtherPeers() {
        return otherPeers;
    }

    public void setOtherPeers(List<Peer> otherPeers) {
        this.otherPeers = otherPeers;
    }

    public PeerSet(List<Peer> set, Peer leader, Peer self) {
        this.set = set;
        this.leader = leader;
        this.self = self;

        otherPeers = new ArrayList<>(set);
        otherPeers.remove(self);
    }

    public List<Peer> getSet() {
        return set;
    }

    public void setSet(List<Peer> set) {
        this.set = set;
    }

    public Peer getLeader() {
        return leader;
    }

    public void setLeader(Peer leader) {
        this.leader = leader;
    }

    public Peer getSelf() {
        return self;
    }

    public void setSelf(Peer self) {
        this.self = self;
    }
}
