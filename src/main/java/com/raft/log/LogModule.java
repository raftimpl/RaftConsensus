package com.raft.log;

import com.raft.pojo.LogEntry;

public interface LogModule {

    void write(LogEntry entry);

    LogEntry read(long index);

    LogEntry getLast();

    Long getLastIndex();

    void updateLastIndex(long index);

    void removeFromIndex(long index);

    void printAll();
}
