package com.raft.log;

import com.raft.pojo.LogEntry;
import com.raft.pojo.Peer;

import java.util.List;

public interface LogModule {

    void write(LogEntry entry);

    LogEntry read(long index);

    LogEntry getLast();

    Long getLastIndex();

    void updateLastIndex(long index);

    void removeFromIndex(long index);

    void printAll();

    long getLastSnapshotIndex();

    void removeRange(long start, long end);

    int getLastSnapshotTerm();

    void updateLastSnapshotIndex(long index);

    void updateLastSnapshotTerm(int term);

    List<Peer> getOtherPeerSet();

    void updateOtherPeerSet(List<Peer> otherPeerSet);
}
