package com.raft;

import com.raft.log.LogModule;
import com.raft.log.LogModuleImpl;
import com.raft.pojo.*;
import com.raft.rpc.RPCClient;
import com.raft.rpc.RPCServer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class NodeServer {
    private static final long ELECTION_TIMEOUT = 150; // 基数:150 毫秒
    private static final long MAX_ELECTION_TIMEOUT = 300; // 最大超时时间

    private volatile long prevElectionStamp;
    private volatile long prevHeartBeatStamp;

    private long electionTimeout = 10 * 1000; // 10s 选举过期，重新选举
    private long heartbeatTimeout = 5 * 1000;// 心跳间隔
    // 各节点持久存在
    private volatile int currentTerm;
    private volatile String voteFor; // 为哪一个candidate 投票(IP:port)

    // 各节点上可变信息
    private long commitIndex;    // 已经提交的最大的日志索引，通过读取持久化信息-SegmentLog 得到
    private long lastApplied;    // 已经应用到状态机的最大日志索引

    private static RPCServer rpcServer;
    private static RPCClient rpcClient;
    private static ScheduledExecutorService threadPool;

    private volatile LogModule logModule;

    private PeerSet peerSet;
    private ReentrantLock lock;

    // leader 中不稳定存在
    private Map<Peer, Long> nextIndexMap; // nextIndex.get(id) 表示要发送给 follower=id 的下一个日志条目的索引
    private Map<Peer, Long> matchIndexMap; // matchIndex.get(id) 表示 follower=id  已经匹配的最大日志索引

    private volatile NodeStatus status;

    public NodeServer() {
        this.status = NodeStatus.FOLLOWER;
    }

    public Response handleRequestVote(VoteParam param) {
        System.out.println("handleRequestVote------------: selfTerm=" + currentTerm + ",voteFor=" + voteFor);
        System.out.println(param);

        lock.lock();
        prevElectionStamp = System.currentTimeMillis();// 定时器重置，防止一个 term 出现多个 candidate

        try {
            if (status == NodeStatus.LEADER || param.getTerm() <= currentTerm) { // == 也不投，== 说明当前节点成为 candidate 或者 已经为其他节点投过票
                System.out.println("拒绝投票--------------");
                return VoteResult.no(currentTerm);
            }
//        if (voteFor == null || voteFor == param.getCandidateId()) { 不要根据 voteFor 判断是否可以支持投票，而是根据任期 term> currentTerm 决定是否投票
            LogEntry lastEntry = null;
            if ((lastEntry = logModule.getLast()) != null) {
                if (lastEntry.getTerm() > param.getPrevLogTerm()) {
                    System.out.println("拒绝投票----------------");
                    return VoteResult.no(currentTerm);
                }
                if (lastEntry.getIndex() > param.getPrevLogIndex()) {
                    System.out.println("拒绝投票--------------");
                    return VoteResult.no(currentTerm);
                }
            }
            System.out.println("同意投票-------------------");
            status = NodeStatus.FOLLOWER;
            voteFor = param.getCandidateId();

            return VoteResult.yes(currentTerm);
//            }
        } finally {
            lock.unlock();
        }

    }

    public void init(List<String> ipAddrs, int selfPort, String selfAddr) {
        // 设置节点信息
        peerSet = new PeerSet();
        Peer cur = new Peer(selfAddr, selfPort);
        List<Peer> peers = new ArrayList<>();
        List<Peer> otherPeers = new ArrayList<>();

        for (String ipAddr : ipAddrs) {
            Peer p = new Peer(ipAddr);
            peers.add(p);
            if (!p.equals(cur)) {
                otherPeers.add(p);
            }
        }
        peerSet.setSelf(cur);
        peerSet.setSet(peers);
        peerSet.setOtherPeers(otherPeers);


        // 默认初始状态 follower
        status = NodeStatus.FOLLOWER;
        lock = new ReentrantLock();

        // 创建 rpc server 并启动
        rpcServer = new RPCServer(selfPort, this);
        rpcServer.start();
        rpcClient = new RPCClient();

        // 创建日志模块
        logModule = new LogModuleImpl();

        // 创建线程池，执行各项任务
        threadPool = Executors.newScheduledThreadPool(3);

        // 启动定时周期性选举任务
        threadPool.scheduleAtFixedRate(new ElectionTask(), 30000, 3000, TimeUnit.MILLISECONDS); // 延迟30s执行，每隔3s 检查是否需要重新选举
        // 启动定时周期性心跳任务
        threadPool.scheduleAtFixedRate(new HeartbeatTask(), 30000, 1000, TimeUnit.MILLISECONDS); // 延迟 30s 执行，每隔 1s 检查是否需要发送心跳

        // 启动输出任务, 每隔 3s 打印当前节点存储的日志项
        threadPool.scheduleAtFixedRate(new PrintTask(), 40000, 10000, TimeUnit.MILLISECONDS);

        // 获取当前任期
        LogEntry entry = logModule.getLast();
        if (entry != null) {
            currentTerm = entry.getTerm();
        }
        // entry==null, 说明任期为 0, 默认值
    }

    class ElectionTask implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                if (status == NodeStatus.LEADER)
                    return;
//                System.out.println("election task");
                long current = System.currentTimeMillis();
                if (current - prevElectionStamp < electionTimeout) {
                    // 选举时间未超时，不需要重新选举
                    return;
                }
                Date date = new Date(System.currentTimeMillis());
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");
                String time = format.format(date);
                System.out.println("-------------------------开始选举-----" + time + "------------------------------");
                // 选举超时，重新选举
                // 1. 更新状态
                status = NodeStatus.CANDIDATE;
                electionTimeout = electionTimeout + (int) (Math.random() * 5000); // 超时之后，延长超时时间，但仍然是随机
                prevElectionStamp = System.currentTimeMillis(); // 设置新的选举时间
                currentTerm = currentTerm + 1;
                voteFor = peerSet.getSelf().getAddr();

                // 2. 获取其他节点信息, 向其他各个节点发送 投票请求
                List<Peer> otherPeers = peerSet.getOtherPeers();

                System.out.println("otherPeers:" + otherPeers);

                LogEntry lastEntry = logModule.getLast();

                List<Future<Response>> resp = new ArrayList<>();
                Future<Response> future = null;

                for (Peer peer : otherPeers) {

                    future = threadPool.submit(new Callable<Response>() {
                        @Override
                        public Response call() throws Exception {
                            VoteParam param = new VoteParam();
                            param.setTerm(currentTerm);
                            param.setCandidateId(peerSet.getSelf().getAddr());
                            param.setPrevLogIndex(lastEntry != null ? lastEntry.getIndex() : -1);
                            param.setPrevLogTerm(lastEntry != null ? lastEntry.getTerm() : -1);

                            Request<VoteParam> voteRequest = new Request<>();
                            voteRequest.setDesc("request-id=向" + peer.getAddr() + "发送投票");
                            voteRequest.setReqObj(param);
                            voteRequest.setType(Request.RequestType.VOTE);
                            voteRequest.setUrl(peer.getAddr());

//                            System.out.println("向" + peer.getAddr() + "发送投票请求");
                            return rpcClient.send(voteRequest);
                        }
                    });
                    resp.add(future);
                }

                CountDownLatch latch = new CountDownLatch(resp.size());

                AtomicInteger countYes = new AtomicInteger(1);
                // 处理返回结果
                for (Future<Response> f : resp) {
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                VoteResult v = (VoteResult) f.get();
                                if (v.isVoteGranted()) {
                                    countYes.incrementAndGet();
                                } else {
                                    // 可能需要更新 term
                                    if (v.getTerm() > currentTerm) {
                                        currentTerm = v.getTerm();
                                    }
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }
                try {
                    latch.await(5000, TimeUnit.MILLISECONDS);// 发送信息到返回结果超时时间 4s
                } catch (InterruptedException e) {
                    System.out.println("等待超时");// 超时不抛出，由于少部分节点一直没响应，但是投票结果已经达到多数，则仍然可以成为 leader
                }
                if (status == NodeStatus.FOLLOWER) {
                    // 在等待期间，同意其他candidate 的投票，或者收到其他已经被选为 leader 的心跳/日志项，当前节点变为 follower
                    System.out.println("等待投票结果期间，已经收到其他被确认为 leader=" + peerSet.getLeader().getAddr() + " 的心跳");
                    // 成为 follower, voteFor 为leaderID
                    return;
                }
                System.out.println("countYes=" + countYes);

                if (countYes.get() > peerSet.getSet().size() / 2) {
                    // 当前节点成为  LEADER
                    status = NodeStatus.LEADER;
                    peerSet.setLeader(peerSet.getSelf());
                    initNextMatchIndex();
                    System.out.println("当前节点成为 leader ");
                } else {
                    System.out.println("当前节点不能成为 leader,重新选举");
                }
                voteFor = null; // 1. 成为leader后，设置 voteFor=null;2.既没成为leader，也没成为follower,设为 null

            } catch (Exception e) {
                System.out.println("error happen in election");
            } finally {
                lock.unlock();
            }

        }
    }

    /**
     * 当前节点成为 leader 后，初始化 nextIndex[] , matchIndex[]
     */
    public void initNextMatchIndex() {
        nextIndexMap = new HashMap<>();
        matchIndexMap = new HashMap<>();
        List<Peer> otherPeers = peerSet.getOtherPeers();
        long lastIndex = logModule.getLastIndex();
        for (Peer p : otherPeers) {
            nextIndexMap.put(p, lastIndex + 1);
            matchIndexMap.put(p, -1L);
        }
    }

    /**
     * 接收到心跳如何处理
     *
     * @param reqObj
     * @return
     */
    public Response handleHeartbeat(AppendEntryParam reqObj) {
        lock.lock();
        try {
            if (reqObj.getTerm() < currentTerm) {
                return AppendEntryResult.no(currentTerm);
            }
            // 更新时间戳
            prevElectionStamp = System.currentTimeMillis();
            prevHeartBeatStamp = System.currentTimeMillis();
//            System.out.println("接受到来自 " + reqObj.getLeaderId() + " 的心跳 term=" + reqObj.getTerm() + ", " + System.currentTimeMillis());

            // 设置主从关系
            peerSet.setLeader(new Peer(reqObj.getLeaderId()));
            if (status != NodeStatus.FOLLOWER) {
                status = NodeStatus.FOLLOWER;
            }
            if (currentTerm != reqObj.getTerm()) {
                currentTerm = reqObj.getTerm();
            }
        } finally {
            lock.unlock();
        }
        return AppendEntryResult.yes(currentTerm);
    }

    class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            try {
                if (status != NodeStatus.LEADER)
                    return;
                long current = System.currentTimeMillis();
                if (current - prevHeartBeatStamp < heartbeatTimeout) { // 每隔 heartBeatTimeOut 才发送新的心跳
                    return;
                }
                prevHeartBeatStamp = System.currentTimeMillis(); // 设置当前发送心跳的时刻

                // 向所有其他follower 发送心跳，心跳参数只需要设置必要的几个
//                System.out.println("--------------发送心跳-------------------------------");
                AppendEntryParam param = new AppendEntryParam();
                param.setEntries(null);
                param.setLeaderId(peerSet.getSelf().getAddr());
                param.setTerm(currentTerm);

                List<Peer> otherPeers = peerSet.getOtherPeers();
//                System.out.println("otherpeers:" + otherPeers);

                for (Peer peer : otherPeers) {
                    Request<AppendEntryParam> request = new Request<>();
                    request.setReqObj(param);
                    request.setType(Request.RequestType.HEARTBEAT);
                    request.setUrl(peer.getAddr());
                    request.setDesc("request-id = 向" + request.getUrl() + "发送心跳, time=" + System.currentTimeMillis());
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            AppendEntryResult resp = (AppendEntryResult) rpcClient.send(request);
                            if (resp == null) {
                                System.out.println(request.getDesc() + " =>心跳失败");
                                return;
                            }
                            if (resp.getTerm() > currentTerm) {
                                status = NodeStatus.FOLLOWER;
                                currentTerm = resp.getTerm();
                                voteFor = null;
                                System.out.println(request.getDesc() + " =>心跳失败");
                            } // else 不处理
                            else {
//                                System.out.println(request.getDesc() + " =>心跳成功");
                            }
                        }
                    });
                }

            } catch (Exception e) {
                System.out.println("heartbeat 出现异常");
            }
        }

    }

    public Response handleGetRequest(Request<Command> request) {
        String key = request.getReqObj().getKey();

        // 从状态机中查询 key 得到 LogEntry
        return ClientResp.yes(null);
    }

    public Response redirect(Request<Command> request) {
        if (peerSet.getLeader() == null)
            return ClientResp.no("no leader");
        request.setUrl(peerSet.getLeader().getAddr());
        System.out.println("redirect to :" + peerSet.getLeader().getAddr());
        return rpcClient.send(request);
    }


    // 多个客户端同时发送请求，需要同步处理
    public synchronized Response handleClientRequest(Request<Command> request) {
        if (status != NodeStatus.LEADER) {
            return redirect(request);
        }
        // 当前节点是 leader

        // get 查询请求
        if (request.getReqObj().getType() == Command.GET) {
            return handleGetRequest(request);
        }
        System.out.println("currentTerm:" + currentTerm);

        // put 请求
        System.out.println(System.currentTimeMillis() + "-------" + request.getReqObj());
        LogEntry entry = new LogEntry(currentTerm, request.getReqObj()); // 日志项的 index 在写入时设置
        // 首先存入 leader 本地
        logModule.write(entry);

        // 向其他节点发送 appendEntryRPC
        List<Peer> otherPeers = peerSet.getOtherPeers();
        List<Future<Boolean>> resultList = new ArrayList<>();
        for (Peer p : otherPeers) {
            Future<Boolean> res = threadPool.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {

                    long begin = System.currentTimeMillis(), end = begin;
                    while (end - begin < 20 * 1000L) {
                        AppendEntryParam param = new AppendEntryParam();
                        param.setTerm(currentTerm);
                        param.setLeaderId(peerSet.getSelf().getAddr());
                        param.setLeaderCommitIndex(commitIndex);

                        // 将[nextIndex, newEntryIndex] 日志全部发送出去
                        List<LogEntry> entryList = new ArrayList<>();
                        Long nextIndex = nextIndexMap.get(p);
                        if (entry.getIndex() >= nextIndex) {
                            for (long i = nextIndex; i <= entry.getIndex(); i++) {
                                LogEntry e = logModule.read(i);
                                if (e != null) {
                                    entryList.add(e);
                                }
                            }
                        } else {
                            entryList.add(entry);
                        }
                        param.setEntries(entryList);

                        // 注意: prevLogIndex prevLogTerm 表示的是即将发送的所有日志项的 前一个日志
                        LogEntry lastEntry = logModule.read(nextIndex - 1); // 因为当前日志中已经添加了 新的日志项
                        if (lastEntry != null) {
                            param.setPrevLogTerm(lastEntry.getTerm());
                            param.setPrevLogIndex(lastEntry.getIndex());
                        }
                        Request<AppendEntryParam> req = new Request<>();
                        req.setUrl(p.getAddr());
                        req.setReqObj(param);
                        req.setType(Request.RequestType.APPEND_ENTRY);
                        req.setDesc("向" + p.getAddr() + "发送" + entry);
                        AppendEntryResult tmpRes = (AppendEntryResult) rpcClient.send(req); // 阻塞式发送 appendEntryRPC
                        if (tmpRes != null) {
                            if (tmpRes.isSuccess()) { // 对方复制成功
                                nextIndexMap.put(p, entry.getIndex() + 1);
                                matchIndexMap.put(p, entry.getIndex());
                                System.out.println(p.getAddr() + " 复制成功");
                                return true;
                            } else { // 复制失败
                                if (tmpRes.getTerm() > currentTerm) {   // 对方任期比自己大
                                    currentTerm = tmpRes.getTerm();
                                    status = NodeStatus.FOLLOWER;
                                    return false;
                                } else {   // 对方任期不比自己大，但失败了, 说明日志不匹配
//                                System.out.println("removeFromIndex:" + nextIndex);
                                    System.out.println("下一次发送给" + p.getAddr() + "的是:" + (nextIndex - 1) + "及之后的数据");
//                                logModule.removeFromIndex(nextIndex);
                                    nextIndexMap.put(p, nextIndex - 1);
                                    // 继续尝试
                                }

                            }
                            end = System.currentTimeMillis();
                        } else {
                            // tmpRes==null 说明对方宕机，连接失败, 不要进行不必要的尝试
                            return false;
                        }
                    }
                    System.out.println(p.getAddr() + " 复制失败");
                    return false;
                }
            });

            resultList.add(res);
        }
        // 得到结果
        CountDownLatch latch = new CountDownLatch(resultList.size());
        AtomicInteger countYes = new AtomicInteger(1);
        for (Future<Boolean> f : resultList) {
            threadPool.execute(() -> {
                try {
                    if (f != null) {  // 当某个节点宕机， f ==null
                        Boolean t = f.get(); // 阻塞，等待返回结果
                        if (t) {
                            countYes.incrementAndGet();
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(5 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("countYes=" + countYes.get());
        if (countYes.get() > peerSet.getSet().size() / 2) {
            // 认为复制成功
            commitIndex = entry.getIndex();

            // TODO: 2019/4/14  尝试提交到状态机
            return ClientResp.yes("提交成功");
        } else {
            // 复制失败,同时删除 leader 下的此日志
            logModule.removeFromIndex(entry.getIndex());

            // 尝试根据 matchIndexMap 更新 commitIndex
            List<Long> matchIndexes = new ArrayList<>(matchIndexMap.values());
            Collections.sort(matchIndexes);
            long mid = matchIndexes.get(matchIndexes.size() / 2);
            if (mid > commitIndex) {
                LogEntry e = logModule.read(mid);
                if (e != null) {
                    if (e.getTerm() == currentTerm) {
                        commitIndex = mid;
                    }
                }
            }

            return ClientResp.no("提交失败");
        }
    }

    public Response handleAppendEntry(AppendEntryParam param) {
        lock.lock();
        try {
            if (param.getTerm() < currentTerm)
                return AppendEntryResult.no(currentTerm);
            System.out.println("-----------handleAppendEntry-----------");
            System.out.println(param);

            prevHeartBeatStamp = System.currentTimeMillis();
            prevElectionStamp = System.currentTimeMillis();
            peerSet.setLeader(new Peer(param.getLeaderId()));
            if (status != NodeStatus.FOLLOWER) {
                status = NodeStatus.FOLLOWER;
            }
            if (currentTerm != param.getTerm()) {
                currentTerm = param.getTerm();
            }
//            System.out.println("1");
            if (param.getPrevLogIndex() != -1 && logModule.getLastIndex() != -1) { // 说明当前节点有日志
                LogEntry matchEntry = logModule.read(param.getPrevLogIndex());
                if (matchEntry != null) {
                    if (matchEntry.getTerm() != param.getPrevLogTerm()) {// prevLogIndex 处的任期号 != prevLogTerm
//                        System.out.println("2");
                        return AppendEntryResult.no(currentTerm); // 返回 false, 让 leader 的 index-1
                    }
                } else {
//                    System.out.println("3");
                    // 在当前找不到  prevLogIndex
                    return AppendEntryResult.no(currentTerm);
                }
            }
            // 第一次添加日志 或者 prevLog 在当前节点中找到匹配的
            int i = 0;

            for (; i < param.getEntries().size(); i++) {
                LogEntry add = param.getEntries().get(i);
                LogEntry entry = logModule.read(add.getIndex());
                if (entry != null) {
                    if (entry.getTerm() != add.getTerm()) {
                        // 要添加的日志项，和已经存在的日志冲突(index 相同但任期号不同)，删除已经存在的日志和后面的日志
                        // 删除 existEntry 及之后的日志项
                        System.out.println("removeIndex:" + add.getIndex());
                        logModule.removeFromIndex(add.getIndex());
                        break; // 删除该位置及之后的日志，跳出循环，从 entry(i).getIndex() 处存储日志
                    }
                    // == ，不用添加，继续遍历比较下一个日志
                } else { // entry == null , 说明从 entry(i).getIndex() 处开始全部添加到当前节点的日志中
                    System.out.println("entry==null");
                    break;
                }
            }

            for (; i < param.getEntries().size(); i++) {
//                System.out.println("for: " + param.getEntries().get(i));
                logModule.write(param.getEntries().get(i));
            }

            // 更新 leaderCommit
            if (param.getLeaderCommitIndex() > commitIndex) {
                commitIndex = Math.min(param.getLeaderCommitIndex(), logModule.getLastIndex());
//                lastApplied = commitIndex;
            }

//            System.out.println("4");

            return AppendEntryResult.yes(currentTerm);

        } finally {
            lock.unlock();
        }


    }

    public void destroy() {
        rpcServer.stop();
    }

    /**
     * 周期性打印当前节点的 日志信息
     */
    class PrintTask implements Runnable {
        @Override
        public void run() {
            logModule.printAll();
        }
    }

}
