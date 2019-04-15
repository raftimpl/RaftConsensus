package com.raft.pojo;

import java.io.Serializable;

/**
 * created by Ethan-Walker on 2019/4/7
 */
public class VoteResult extends Response implements Serializable {
    private int term;
    private boolean voteGranted;

    public static VoteResult yes(int term) {
        VoteResult result = new VoteResult();
        result.term = term;
        result.voteGranted = true;
        return result;
    }

    public static VoteResult no(int term) {
        VoteResult result = new VoteResult();
        result.term = term;
        result.voteGranted = false;
        return result;
    }

    public VoteResult() {
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
