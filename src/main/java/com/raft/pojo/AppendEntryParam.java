package com.raft.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class AppendEntryParam implements Serializable {
    private int term; // leader's term
    private String leaderId;
    private long prevLogIndex = -1;
    private int prevLogTerm = -1;

    private List<LogEntry> entries; // 发送的日志项
    private long leaderCommitIndex;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntry> entries) {
        this.entries = entries;
    }

    public long getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public void setLeaderCommitIndex(long leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }

    @Override
    public String toString() {
        return "AppendEntryParam{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + entries +
                ", leaderCommitIndex=" + leaderCommitIndex +
                '}';
    }
}
