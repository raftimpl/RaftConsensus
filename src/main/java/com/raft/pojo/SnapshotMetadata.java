package com.raft.pojo;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * created by Ethan-Walker on 2019/5/11
 */
public class SnapshotMetadata implements Serializable {
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private LinkedHashMap<String, String> data;

    public SnapshotMetadata() {
    }

    public SnapshotMetadata(long lastIncludedIndex, int lastIncludedTerm, LinkedHashMap<String, String> data) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = data;
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

    public LinkedHashMap<String, String> getData() {
        return data;
    }

    public void setData(LinkedHashMap<String, String> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "SnapshotMetadata{" +
                "lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", data=" + data +
                '}';
    }
}
