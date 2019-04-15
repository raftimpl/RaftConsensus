package com.raft.consensus;

import com.raft.NodeServer;
import com.raft.pojo.*;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class ConsensusImpl implements Consensus {

    private NodeServer nodeServer;

    public ConsensusImpl(NodeServer server) {
        nodeServer = server;
    }

    public VoteResult requestVote(VoteParam param) {
        return null;
    }

    public AppendEntryResult appendEntry(AppendEntryParam param) {
        return null;
    }
}
