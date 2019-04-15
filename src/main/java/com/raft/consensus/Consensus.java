package com.raft.consensus;

import com.raft.pojo.AppendEntryParam;
import com.raft.pojo.AppendEntryResult;
import com.raft.pojo.VoteParam;
import com.raft.pojo.VoteResult;

import java.rmi.RemoteException;

public interface Consensus {

    /**
     * 其他节点发送request vote 请求，封装到param 参数中
     * @param param
     * @return
     * @throws RemoteException
     */
    VoteResult requestVote(VoteParam param) ;

    AppendEntryResult appendEntry(AppendEntryParam param);
}
