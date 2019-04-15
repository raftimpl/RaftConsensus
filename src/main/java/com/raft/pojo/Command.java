package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/7
 */
public class Command implements Serializable {
    public static int PUT = 0;
    public static int GET = 1;
    private int type;
    private String key;
    private String value;

    public Command(String key, String value, int type) {
        this.key = key;
        this.value = value;
        this.type = type;
    }

    public Command(String key, int type) {
        this.key = key;
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Command{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
