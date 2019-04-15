package com.raft.pojo;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class AppendEntryResult extends Response {

    private int term;
    private boolean success;

    public static AppendEntryResult yes(int term){
        AppendEntryResult result = new AppendEntryResult();
        result.term = term;
        result.success = true;
        return result;
    }
    public static AppendEntryResult no(int term){
        AppendEntryResult result = new AppendEntryResult();
        result.term = term;
        result.success = false;
        return result;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
