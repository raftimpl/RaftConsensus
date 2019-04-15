package com.raft.exception;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class TimeoutException extends Throwable {
    public TimeoutException(String msg) {
        super(msg);
    }
}
