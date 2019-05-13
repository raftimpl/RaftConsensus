package com.raft.snapshot;

import com.raft.pojo.SnapshotMetadata;

import java.util.LinkedHashMap;

/**
 * created by Ethan-Walker on 2019/5/10
 */
public interface Snapshot {

    void updateMetadata(long lastIncludedIndex, int lastIncludedTerm, LinkedHashMap<String, String> data);

    SnapshotMetadata getMetadata();

}
