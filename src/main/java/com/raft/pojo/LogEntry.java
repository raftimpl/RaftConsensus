package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/7
 */
public class LogEntry implements Serializable {

    private int term;  // 任期
    private long index; // 当前日志项所在leader的位置
    private Command command;

    public LogEntry() {
    }

    public LogEntry(int term,  Command command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", command=" + command +
                '}';
    }
}
