package com.raft.statemachine;

import com.raft.pojo.LogEntry;

import java.util.LinkedHashMap;

/*
 * 状态机接口
 * */
public interface StateMachine {
    //应用到状态机
    void apply(LogEntry logEntry);

    void store(String key, String value);

    //由key获取value
    String getValue(String key);

    //设置指定key的value

    void setValue(String key, String value);

    //删除多个key对应的value
    void delValue(String... key);

    void print();

    long getLastApplied();

    LinkedHashMap<String, String> getData();
}

