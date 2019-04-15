package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/8
 */
public class VoteParam implements Serializable {
    private int term;
    private String candidateId; // ip:port，候选者让 其他服务器为自己投票
    private long prevLogIndex; // 候选人最新的日志条目索引值
    private long prevLogTerm;   // 候选人最新的日志条目的任期

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    @Override
    public String toString() {
        return "VoteParam{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                '}';
    }
}
