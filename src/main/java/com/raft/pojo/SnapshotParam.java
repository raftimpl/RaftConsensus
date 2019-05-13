package com.raft.pojo;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * created by Ethan-Walker on 2019/5/10
 */
public class SnapshotParam implements Serializable {

    private int term;

    private String leaderId;

    private long lastIncludedIndex;

    private int lastIncludedTerm;

    private int offset;     // 快照中状态机数据的位置

    private LinkedHashMap<String, String> data;

    private boolean done;

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

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(int lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public LinkedHashMap<String, String> getData() {
        return data;
    }

    public void setData(LinkedHashMap<String, String> data) {
        this.data = data;
    }
}
