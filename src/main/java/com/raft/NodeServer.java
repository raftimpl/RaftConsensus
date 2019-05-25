package com.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.raft.log.LogModule;
import com.raft.log.LogModuleImpl;
import com.raft.pojo.*;
import com.raft.rpc.NodeRPCClient;
import com.raft.rpc.NodeRPCServer;
import com.raft.snapshot.Snapshot;
import com.raft.snapshot.SnapshotImpl;
import com.raft.statemachine.StateMachine;
import com.raft.statemachine.StateMachineImpl;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
    private static final Logger LOG = LoggerFactory.getLogger(NodeServer.class);
    private volatile long prevElectionStamp;
    private volatile long prevHeartBeatStamp;

    private long electionTimeout = 10 * 1000; // 10s 选举过期，重新选举
    private long heartbeatTimeout = 5 * 1000;// 心跳间隔
    // 各节点持久存在
    private volatile int currentTerm;
    private volatile String voteFor; // 为哪一个candidate 投票(IP:port)

    private volatile boolean dataChanged = true;
    // 各节点上可变信息
    private long commitIndex;    // 已经提交的最大的日志索引，通过读取持久化信息-SegmentLog 得到
    private long lastApplied;    // 已经应用到状态机的最大日志索引

    private static NodeRPCServer rpcServer;
    private static NodeRPCClient rpcClient;
//    private static ManageRPCClient registerClient;

    private static ScheduledExecutorService threadPool;

    private volatile LogModule logModule;
    private volatile StateMachine stateMachine;
    private volatile Snapshot snapshot;
    //    private String manageServerAddr;
    private PeerSet peerSet;
    private ReentrantLock lock;

    // leader 中不稳定存在
    private Map<Peer, Long> nextIndexMap; // nextIndex.get(id) 表示要发送给 follower=id 的下一个日志条目的索引
    private Map<Peer, Long> matchIndexMap; // matchIndex.get(id) 表示 follower=id  已经匹配的最大日志索引

    private volatile NodeStatus status;
    private ConcurrentHashMap<String, Response> respMap;
    private final int RESPMAP_MAXSIZE = 300; // 当客户端请求数达到 RESPMAP_MAXSIZE 时，清空客户端请求 respMap

    private final int SNAPSHOT_SIZE = 5;  //  当应用到的状态机的日志条目是 SNAPSHOT_SIZE 的整数倍时，进行快照处理，更新元数据

    public NodeServer() {
        this.status = NodeStatus.FOLLOWER;
    }

    public void init(String selfAddr, int selfPort) {

        // 默认初始状态 follower
        status = NodeStatus.FOLLOWER;
        lock = new ReentrantLock();

        commitIndex = -1;
        lastApplied = -1;
        // key=客户端请求唯一标识,value=此次请求的响应
        respMap = new ConcurrentHashMap<>();

        // 创建 rpc server 并启动
        rpcServer = new NodeRPCServer(selfPort, this);
        rpcServer.start();
        rpcClient = new NodeRPCClient();
//        registerClient = new ManageRPCClient();

        // 创建日志模块
        logModule = new LogModuleImpl();
        stateMachine = new StateMachineImpl();
        snapshot = new SnapshotImpl();

        // 设置当前节点信息
        peerSet = new PeerSet();
        Peer cur = new Peer(selfAddr, selfPort);
        peerSet.setSelf(cur);

        List<Peer> otherPeerSet = logModule.getOtherPeerSet();

        if (otherPeerSet == null || otherPeerSet.size() == 0) {
            // 读取cluster.json 文件
            initPeerSet();
        } else {
            peerSet.setOtherPeers(otherPeerSet);
            ArrayList<Peer> peers = new ArrayList<>(otherPeerSet);
            peers.add(cur);
            peerSet.setSet(peers);
        }
        LOG.trace("otherPeers: " + peerSet.getOtherPeers());

        // 获取当前任期
        LogEntry entry = logModule.getLast();
        if (entry != null) {        // entry==null, 说明任期为 0, 默认值
            currentTerm = entry.getTerm();
        }

        // 创建线程池，执行各项任务
        threadPool = Executors.newScheduledThreadPool(4);

        // 启动定时周期性选举任务
        threadPool.scheduleWithFixedDelay(new ElectionTask(), 30000, 3000, TimeUnit.MILLISECONDS); // 延迟30s执行，每隔3s 检查是否需要重新选举

        // 启动定时周期性心跳任务
        threadPool.scheduleWithFixedDelay(new HeartbeatTask(), 33000, 1000, TimeUnit.MILLISECONDS); // 延迟 30s 执行，每隔 1s 检查是否需要发送心跳

        // 启动输出任务, 每隔 3s 打印当前节点存储的日志项
        threadPool.scheduleWithFixedDelay(new PrintTask(), 3000, 3000, TimeUnit.MILLISECONDS);
        // 设置 lastApplied
        commitIndex = lastApplied = stateMachine.getLastApplied();
        dataChanged = true;

        // 启动定时周期性注册当前地址任务
//        threadPool.schedule(new RegisterTask(), 0, TimeUnit.MILLISECONDS);

    }
/*

    public Response register() {
        ClusterRequest<Peer> request = new ClusterRequest<>();
        request.setReqObj(peerSet.getSelf());
        request.setAddr(manageServerAddr);
        request.setRequestType(ClusterRequest.RequestType.REGISTER_NODE);
        return registerClient.send(request);
    }

*/

    public void initPeerSet() {

        List<Peer> otherPeers = new ArrayList<>();
        try {
            String s = FileUtils.readFileToString(new File("./cluster.json"));
            JSONObject jsonObject = JSON.parseObject(s);
            JSONArray servers = jsonObject.getJSONArray("servers");
            for (int i = 0; i < servers.size(); i++) {
                String addr = servers.getJSONObject(i).getString("addr");
                if (!addr.equals(peerSet.getSelf().getAddr())) {
                    otherPeers.add(new Peer(addr));
                }
            }
            // 同时保存到日志文件中
            logModule.updateOtherPeerSet(otherPeers);

        } catch (IOException e) {
            e.printStackTrace();
        }
        peerSet.setOtherPeers(otherPeers);
        ArrayList<Peer> peers = new ArrayList<>(otherPeers);
        peers.add(peerSet.getSelf());
        peerSet.setSet(peers);

    }


    public void updatePeerSetInfo() {
/*
        if (resObj != null) {
            List<Peer> peers = (List<Peer>) resObj;
            List<Peer> otherPeers = new ArrayList<>();
            for (Peer p : peers) {
                if (!p.equals(peerSet.getSelf())) {
                    otherPeers.add(p);
                }
            }
            peerSet.setOtherPeers(otherPeers);
            peerSet.setSet(peers);
        }
*/
    }
/*
    class RegisterTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                Response resp = register();
                if ((boolean) resp.getResObj()) {
                    System.out.println("注册节点成功,跳出循环");
                    break;
                }
                System.out.println("注册节点失败，循环");
            }
            System.out.println(" 签到成功");

            // 启动周期性签到任务
            threadPool.scheduleAtFixedRate(new SignTask(), 3000, 3000, TimeUnit.MILLISECONDS);
        }
    }

    class SignTask implements Runnable {
        @Override
        public void run() {
            ClusterRequest<Peer> request = new ClusterRequest<>();
            request.setReqObj(peerSet.getSelf());
            request.setAddr(manageServerAddr);
            request.setRequestType(ClusterRequest.RequestType.SIGN);
            registerClient.send(request);

        }
    }
*/

    /**
     * 选举之前测试当前节点是否和集群中大多数节点能够成功通信，如果能够，才进行选举
     * 否则可能因为当前节点在较小的网络分区（少于半数节点）导致一直选举失败，term 不断增大
     *
     * @return
     */
    private boolean preVote() {
//        updatePeerSetInfo();
        List<Peer> otherPeers = peerSet.getOtherPeers();
        System.out.println("preVote(): otherPeers=" + otherPeers);
        List<Future<Response>> list = new ArrayList<>();
        for (Peer p : otherPeers) {
            Request pingReq = new Request();
            pingReq.setUrl(p.getAddr());
            pingReq.setType(Request.RequestType.PING);
            Future<Response> future = threadPool.submit(new Callable<Response>() {
                @Override
                public Response call() throws Exception {
                    return rpcClient.send(pingReq);
                }
            });
            list.add(future);
        }
        CountDownLatch latch = new CountDownLatch(otherPeers.size());
        AtomicInteger respCount = new AtomicInteger(1);
        for (Future<Response> f : list) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Response resp = null;
                    try {
                        resp = f.get(3000, TimeUnit.MILLISECONDS);
                        if (resp != null) {
                            respCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });

        }
        try {
            latch.await(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (respCount.get() > otherPeers.size() / 2) {
            System.out.println("preVote result = true");
            return true;
        }
        System.out.println("preVote result = false");
        return false;
    }

    public Response handlePingReq(Request req) {
        Response resp = new Response();
        return resp;
    }

    class ElectionTask implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                if (status == NodeStatus.LEADER)
                    return;
                LOG.trace("election task");
                long current = System.currentTimeMillis();
                if (current - prevElectionStamp < electionTimeout) {
                    // 选举时间未超时，不需要重新选举
                    return;
                }

                if (!preVote()) {
                    return;
                }
                Date date = new Date(System.currentTimeMillis());
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");
                String time = format.format(date);
                System.out.println("-------------------------开始选举-----------------------------------");
                // 选举超时，重新选举
                // 1. 更新状态
                status = NodeStatus.CANDIDATE;
                electionTimeout = electionTimeout + (int) (Math.random() * 5000); // 超时之后，延长超时时间，但仍然是随机
                prevElectionStamp = System.currentTimeMillis(); // 设置新的选举时间
                currentTerm = currentTerm + 1;
                voteFor = peerSet.getSelf().getAddr();

                // updatePeerSetInfo();  preVote 阶段已经更新了

                // 2. 获取其他节点信息, 向其他各个节点发送 投票请求
                List<Peer> otherPeers = peerSet.getOtherPeers();
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
                            voteRequest.setDesc("发送投票");
                            voteRequest.setReqObj(param);
                            voteRequest.setType(Request.RequestType.VOTE);
                            voteRequest.setUrl(peer.getAddr());

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
                                Response response = f.get(3000, TimeUnit.MILLISECONDS);
                                if (response == null) return;
                                VoteResult v = (VoteResult) response;
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
                            } catch (TimeoutException e) {
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
                System.out.println("收到的选票总数:" + countYes);

                if (countYes.get() > peerSet.getSet().size() / 2) {
                    // 当前节点成为  LEADER
                    status = NodeStatus.LEADER;
                    peerSet.setLeader(peerSet.getSelf());
                    initNextMatchIndex();
                    System.out.println("当前节点成为 leader");
                } else {
                    System.out.println("当前节点不能成为 leader,重新选举");
                }
                voteFor = null; // 1. 成为leader后，设置 voteFor=null;2.既没成为leader，也没成为follower,设为 null

            } catch (Exception e) {
                e.printStackTrace();
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

    public Response handleRequestVote(VoteParam param) {
        System.out.println("handleRequestVote: selfTerm=" + currentTerm + "," + param.toString());
        lock.lock();
        prevElectionStamp = System.currentTimeMillis();// 定时器重置，防止一个 term 出现多个 candidate

        try {
            if (status == NodeStatus.LEADER || param.getTerm() <= currentTerm) { // == 也不投，== 说明当前节点成为 candidate 或者 已经为其他节点投过票
                System.out.println("拒绝投票");
                return VoteResult.no(currentTerm);
            }
//        if (voteFor == null || voteFor == param.getCandidateId()) { 不要根据 voteFor 判断是否可以支持投票，而是根据任期 term> currentTerm 决定是否投票
            // param.getTerm() > currentTerm, 还要判断candidate节点日志是否比当前节点新
            LogEntry lastEntry = null;
            if ((lastEntry = logModule.getLast()) != null) {
                if (lastEntry.getTerm() > param.getPrevLogTerm() || lastEntry.getIndex() > param.getPrevLogIndex()) {
                    System.out.println("拒绝投票:candidate 节点日志没有当前节点全");
                    return VoteResult.no(currentTerm);
                }
            }
            currentTerm = param.getTerm();
            System.out.println("同意投票");
            status = NodeStatus.FOLLOWER;
            voteFor = param.getCandidateId();
            return VoteResult.yes(currentTerm);
//         }
        } finally {
            lock.unlock();
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

            LOG.trace("handleHeartbeat start: from " + reqObj.getLeaderId() + "---prevLogIndex=" + reqObj.getPrevLogIndex() + ",prevLogTerm=" + reqObj.getPrevLogTerm() + "-------");
            // 设置主从关系
            peerSet.setLeader(new Peer(reqObj.getLeaderId()));
            if (status != NodeStatus.FOLLOWER) {
                status = NodeStatus.FOLLOWER;
            }
            if (currentTerm != reqObj.getTerm()) {
                currentTerm = reqObj.getTerm();
            }

            // 检验当前日志是否缺少 (重启后的节点很有可能缺少部分日志项)
            long prevLogIndex = reqObj.getPrevLogIndex();
            if (prevLogIndex != -1) { // 说明leader 中有日志
                LOG.trace("lastSnapshotIndex=" + logModule.getLastSnapshotIndex());
                if (prevLogIndex <= logModule.getLastSnapshotIndex()) {
                    LOG.trace("prevLogIndex <= logModule.getLastSnapshotIndex() ");
                    // 不缺少日志，不需要处理，说明当前节点 prevLogIndex 对应的位置已经生成快照，应用到状态机
                } else {
                    LogEntry entry = logModule.read(prevLogIndex);
                    if (entry == null || entry.getTerm() != reqObj.getPrevLogTerm()) {
                        // 返回给心跳一个标记，提醒让leader 启动发送日志的过程（只用发送给当前节点即可）
                        System.out.println("当前节点缺少日志");
                        return AppendEntryResult.yes(Integer.MIN_VALUE); // term =  MIN_VALUE 为特殊标记
                    }
                }
            }
            commitIndex = reqObj.getLeaderCommitIndex();

            //Follower收到心跳后应用状态机lastIndex
            LogEntry newLogEntry;
            if (lastApplied < commitIndex) {
                LOG.trace("检测到需要提交[ lastApplied = " + lastApplied + " , commitIndex = " + commitIndex + " ] 范围的日志");
                for (long i = lastApplied + 1; i <= commitIndex; i++) {
                    newLogEntry = logModule.read(i);
                    stateMachine.apply(newLogEntry);
                    lastApplied = i;
                    // 可能一次apply多条日志，可能中间有一条日志恰到达
                    trySnapshot();
                    dataChanged = true;
                }
            }
        } finally {
            lock.unlock();
        }
        return AppendEntryResult.yes(currentTerm);
    }

    public void trySnapshot() {
        if (lastApplied != -1 && (lastApplied + 1) % SNAPSHOT_SIZE == 0) {
            // 更新快照
            LinkedHashMap<String, String> data = stateMachine.getData();
            int lastSnapshotTerm = logModule.read(lastApplied).getTerm();

            snapshot.updateMetadata(lastApplied, lastSnapshotTerm, data);
            // 删除 (..., lastApplied] 日志
            long lastSnapshotIndex = logModule.getLastSnapshotIndex();
            logModule.removeRange(lastSnapshotIndex + 1, lastApplied);
            logModule.updateLastSnapshotTerm(lastSnapshotTerm);
            logModule.updateLastSnapshotIndex(lastApplied);
        }
    }

    class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            try {
                if (status != NodeStatus.LEADER)
                    return;
                if (respMap.size() > RESPMAP_MAXSIZE) {  // 清空
                    respMap.clear();
                }
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
                param.setLeaderCommitIndex(commitIndex);
                // 这里的 prevLogIndex/term 就设置成 lastEntry 的index/term, 不需要设置成 nextIndex 的 prevIndex, 因为发送日志不走这, nextIndex-- 不会在这起到作用
                LogEntry lastEntry = logModule.getLast();
                if (lastEntry != null) {
                    param.setPrevLogIndex(lastEntry.getIndex());
                    param.setPrevLogTerm(lastEntry.getTerm());
                }

                List<Peer> otherPeers = peerSet.getOtherPeers();
//                System.out.println("otherpeers:" + otherPeers);

//                updatePeerSetInfo();

                for (Peer peer : otherPeers) {
                    Request<AppendEntryParam> request = new Request<>();
                    request.setReqObj(param);
                    request.setType(Request.RequestType.HEARTBEAT);
                    request.setUrl(peer.getAddr());
                    request.setDesc("发送心跳");
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {

                            AppendEntryResult resp = (AppendEntryResult) rpcClient.send(request);
                            if (resp == null) {
                                LOG.warn(request.getDesc() + " => 心跳失败1");
                                return;
                            }
                            if (resp.getTerm() > currentTerm) {
                                status = NodeStatus.FOLLOWER;
                                currentTerm = resp.getTerm();
                                voteFor = null;
                                LOG.warn(request.getDesc() + " => 心跳失败2");
                            } // else 不处理
                            else if (resp.getTerm() == Integer.MIN_VALUE) { // 说明目标节点缺少部分日志
                                System.out.println("发现目标节点" + peer.toString() + "缺少部分日志,开始同步日志");
                                if (sendAppendEntry(peer, logModule.getLastIndex())) {
                                    System.out.println("更新成功");
                                } else {
                                    System.out.println("更新失败");
                                }
                            } else {
//                                System.out.println(request.getDesc() + " =>心跳成功");
                            }
                        }
                    });
                }
            } catch (Exception e) {
                System.out.println("连接失败");
                e.printStackTrace();
            }
        }
    }


    public Response handleGetRequest(Request<Command> request) {
        String key = request.getReqObj().getKey();
        // 从状态机中查询 key 得到 LogEntry
        String value = stateMachine.getValue(key);
        return ClientResp.yes(value);
    }

    public Response redirect(Request request) {
        if (peerSet.getLeader() == null) {
            return ClientResp.no(false);

        }
        request.setUrl(peerSet.getLeader().getAddr());
        System.out.println("redirect to :" + peerSet.getLeader().getAddr());
        Response resp = rpcClient.send(request);
        respMap.put(request.getId(), resp);
        return resp;
    }

    /**
     * put 请求只允许一个接一个提交，防止出现一个节点中 a=3,a=2;  另一个节点出现a=2,a=3 的情况
     *
     * @param request
     * @return
     */
    public synchronized Response handlePutClientRequest(Request<Command> request) {

        System.out.println("\nhandlePutClientRequest: " + request.getReqObj());
        LogEntry entry = new LogEntry(currentTerm, request.getReqObj()); // 日志项的 index 在写入时设置
        // 首先存入 leader 本地
        logModule.write(entry);

        // 向其他节点发送 appendEntryRPC
        List<Peer> otherPeers = peerSet.getOtherPeers();
        List<Future<Boolean>> resultList = new ArrayList<>();

//        updatePeerSetInfo();

        for (Peer p : otherPeers) {
            Future<Boolean> res = threadPool.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return sendAppendEntry(p, entry.getIndex());
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
        System.out.println("添加日志成功的节点总数 = " + countYes.get());
        if (countYes.get() > peerSet.getSet().size() / 2) {
            // 认为复制成功
            commitIndex = entry.getIndex();

            //提交到状态机
            LogEntry newLogEntry;
            if (lastApplied < commitIndex) LOG.trace("提交状态机");
            for (long i = lastApplied + 1; i <= commitIndex; i++) {
                newLogEntry = logModule.read(i);
                LOG.trace(newLogEntry.getCommand().toString());
                stateMachine.apply(newLogEntry);
            }
            lastApplied = commitIndex;
//            stateMachine.printAll();
            LOG.trace("last Applied=" + lastApplied);
            trySnapshot();
            dataChanged = true;
            return ClientResp.yes(true);
        } else {
            // 复制失败,同时删除 leader 下的此日志之后的所有日志
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
            LogEntry newLogEntry;
            for (long i = lastApplied + 1; i <= commitIndex; i++) {
                newLogEntry = logModule.read(i);
//                System.out.println(newLogEntry.getCommand().toString());
                stateMachine.apply(newLogEntry);
            }
            lastApplied = commitIndex;
            trySnapshot();
            return ClientResp.no(false);
        }
    }

    // 多个客户端同时发送请求，需要同步处理
    public Response handleClientRequest(Request<Command> request) {
        if (status != NodeStatus.LEADER) {
            return redirect(request);
        }
        // 当前节点是 leader

        // 当前请求已经处理过
        if (respMap.containsKey(request.getId())) {
            return respMap.get(request.getId());
        }

        // get 查询请求
        if (request.getReqObj().getType() == Command.GET) {
            return handleGetRequest(request);
        }

        return handlePutClientRequest(request);

    }

    public boolean sendInstallSnapshotRPC(Peer p) {
        System.out.println("\nsendInstallSnapshotRPC:");
        Request<SnapshotParam> installSnapshot = new Request<>();
        installSnapshot.setType(Request.RequestType.INSTALL_SNAPSHOT);
        installSnapshot.setUrl(p.getAddr());

        SnapshotParam param = new SnapshotParam();
        param.setTerm(currentTerm);
        param.setDone(true);
        param.setLeaderId(peerSet.getSelf().getAddr());
        SnapshotMetadata metadata = snapshot.getMetadata();
        param.setData(metadata.getData());
        param.setLastIncludedIndex(metadata.getLastIncludedIndex());
        param.setLastIncludedTerm(metadata.getLastIncludedTerm());
        param.setOffset(0);

        installSnapshot.setReqObj(param);

        Response resp = rpcClient.send(installSnapshot);

        int term = (int) resp.getResObj();
        if (term > currentTerm) {
            currentTerm = term;
            status = NodeStatus.FOLLOWER;
            return false;
        }

//        printData();
        return true;
    }

    public Response handleInstallSnapshotRPC(Request<SnapshotParam> request) {
        System.out.println("\nhandleInstallSnapshotRPC:");
        SnapshotParam param = request.getReqObj();
        Response resp = new Response();

        if (param.getTerm() < currentTerm) {
            resp.setObj(currentTerm);
            return resp;
        }
        currentTerm = param.getTerm();
        resp.setObj(currentTerm);

        snapshot.updateMetadata(param.getLastIncludedIndex(), param.getLastIncludedTerm(), param.getData());
        logModule.removeRange(logModule.getLastSnapshotIndex() + 1, logModule.getLastIndex());
        logModule.updateLastSnapshotIndex(param.getLastIncludedIndex());
        logModule.updateLastIndex(param.getLastIncludedIndex());
        logModule.updateLastSnapshotTerm(param.getLastIncludedTerm());

        lastApplied = param.getLastIncludedIndex();
        commitIndex = param.getLastIncludedIndex();

        // 状态机存储快照内的内容
        LinkedHashMap<String, String> data = param.getData();
        Set<Map.Entry<String, String>> entries = data.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            stateMachine.store(entry.getKey(), entry.getValue());
        }
        dataChanged = true;
        return resp;
    }

    public boolean sendAppendEntry(Peer p, long lastIndex) {
        LOG.trace("----------------sendAppendEntry to:" + p + "----------------------");
        // 参数 peer , lastIndex,
        long begin = System.currentTimeMillis(), end = begin;
        while (end - begin < 20 * 1000L) {
            // 将[nextIndex, newEntryIndex] 日志全部发送出去
            List<LogEntry> entryList = new ArrayList<>();
            long nextIndex = nextIndexMap.getOrDefault(p, lastIndex);
            // 如果 nextIndex <= lastIncludedIndex(在快照中，发送快照信息)
            LOG.trace("nextIndex=" + nextIndex + ",logModule.getLastSnapshotIndex()=" + logModule.getLastSnapshotIndex() + ",lastIndex=" + lastIndex);

            if (nextIndex > lastIndex) { // 说明不是新添加的日志导致调用该方法，而是检测到缺少日志
                System.out.println(nextIndex + ">" + lastIndex);
                nextIndex = lastIndex;
                nextIndexMap.put(p, nextIndex);
            }

            if (nextIndex <= logModule.getLastSnapshotIndex()) {
                LOG.warn("1");
                sendInstallSnapshotRPC(p);
                nextIndex = logModule.getLastSnapshotIndex() + 1;
                nextIndexMap.put(p, nextIndex);
            }

            AppendEntryParam param = new AppendEntryParam();
            param.setTerm(currentTerm);
            param.setLeaderId(peerSet.getSelf().getAddr());
            param.setLeaderCommitIndex(commitIndex);

//          if (entry.getIndex() >= nextIndex) {
            for (long i = nextIndex; i <= lastIndex; i++) {
                LogEntry e = logModule.read(i);
                if (e != null) {
                    entryList.add(e);
                }
            }
//          } else {
//                entryList.add(entry);
//          }
            param.setEntries(entryList);

            // 注意: prevLogIndex prevLogTerm 表示的是即将发送的所有日志项的 前一个日志
            LogEntry prevEntry = logModule.read(nextIndex - 1); // 因为当前日志中已经添加了 新的日志项
            if (prevEntry != null) {
                param.setPrevLogTerm(prevEntry.getTerm());
                param.setPrevLogIndex(prevEntry.getIndex());
            }
            Request<AppendEntryParam> req = new Request<>();
            req.setUrl(p.getAddr());
            req.setReqObj(param);
            req.setType(Request.RequestType.APPEND_ENTRY);
//            req.setDesc("向" + p.getAddr() + "发送" + entry);
            AppendEntryResult tmpRes = (AppendEntryResult) rpcClient.send(req); // 阻塞式发送 appendEntryRPC
            if (tmpRes != null) {
                if (tmpRes.isSuccess()) { // 对方复制成功
                    nextIndexMap.put(p, lastIndex + 1);
                    matchIndexMap.put(p, lastIndex);
                    LOG.trace(p.getAddr() + " 复制成功");
                    return true;
                } else { // 复制失败
                    if (tmpRes.getTerm() > currentTerm) {   // 对方任期比自己大
                        currentTerm = tmpRes.getTerm();
                        status = NodeStatus.FOLLOWER;
                        return false;
                    } else {   // 对方任期不比自己大，但失败了, 说明日志不匹配
//                                System.out.println("removeFromIndex:" + nextIndex);
                        System.out.println("下一次发送给" + p.getAddr() + "的是:" + (nextIndex - 1) + "及之后的数据");
//                      logModule.removeFromIndex(nextIndex);  不应该删除，只减少 nextIndex 即可
                        nextIndexMap.put(p, nextIndex - 1);
                        // 继续尝试
                    }
                }
                end = System.currentTimeMillis();
            } else {
                // tmpRes==null 说明对方宕机，连接失败, 不要进行不必要的尝试
                System.out.println(p.getAddr() + " 复制失败");
                return false;
            }
        }
        System.out.println(p.getAddr() + " 复制失败");
        return false;
    }

    public synchronized Response handleAppendEntry(AppendEntryParam param) {
        if (param.getTerm() < currentTerm)
            return AppendEntryResult.no(currentTerm);
        System.out.println("\nhandleAppendEntry start-----------");
        System.out.println(param.toString());

        prevHeartBeatStamp = System.currentTimeMillis();
        prevElectionStamp = System.currentTimeMillis();
        peerSet.setLeader(new Peer(param.getLeaderId()));
        if (status != NodeStatus.FOLLOWER) {
            status = NodeStatus.FOLLOWER;
        }
        if (currentTerm != param.getTerm()) {
            currentTerm = param.getTerm();
        }
        if (param.getEntries().size() == 0) { // 已经接受到压缩快照，压缩快照后面恰好已经没有日志了
            return AppendEntryResult.yes(currentTerm);
        }

        if (logModule.getLastIndex() == -1) {
            if (param.getPrevLogIndex() != -1) {
                return AppendEntryResult.no(currentTerm);
            } else {
                // 复制
            }
        } else {
            // !=-1，当前节点有日志
            if (param.getPrevLogIndex() == -1) {
                return AppendEntryResult.no(currentTerm);
            } else {
                LogEntry matchEntry = logModule.read(param.getPrevLogIndex());
                System.out.println("matchEntry:" + matchEntry);
                if (matchEntry != null) {
                    if (matchEntry.getTerm() != param.getPrevLogTerm()) {// prevLogIndex 处的任期号 != prevLogTerm
//                        System.out.println("2");
                        return AppendEntryResult.no(currentTerm); // 返回 false, 让 leader 的 index-1
                    } else {
                        // 找到：复制
                        LOG.warn("找到：复制");
                    }
                } else {
                    LOG.warn("3");
                    // 在当前节点找不到  prevLogIndex
                    return AppendEntryResult.no(currentTerm);
                }
            }
        }

        // 第一次添加日志 或者 prevLog 在当前节点中找到匹配的

        for (int i = 0; i < param.getEntries().size(); i++) {
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
            } else { // entry == null , 说明从 param.getEntries().get(i) 处开始全部添加到当前节点的日志中
                break;
            }
        }

        for (int i = 0; i < param.getEntries().size(); i++) {
//                System.out.println("for: " + param.getEntries().get(i));
            LOG.trace("write " + param.getEntries().get(i));
            logModule.update(param.getEntries().get(i));
        }
//        printData();
        return AppendEntryResult.yes(currentTerm);

    }


    /**
     * TODO: 2019/5/14   成员变更
     * 添加节点，一次只能添加一个节点(synchronized)
     *
     * @param request
     * @return
     */
    public synchronized Response handleAddServer(Request<String> request) {
        if (status != NodeStatus.LEADER) {
            return redirect(request);
        }
        // 当前节点是 leader

        // 当前请求已经处理过
        if (respMap.containsKey(request.getId())) {
            return respMap.get(request.getId());
        }
        List<Peer> otherPeerSet = logModule.getOtherPeerSet();
        HashSet<Peer> peers = new HashSet<>(otherPeerSet);
        peers.add(new Peer(request.getReqObj()));

        return null;
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
            printData();
        }
    }

    private void printData() {
        if (dataChanged) {
            dataChanged = false;
            System.out.println("\n");
            System.out.println("---------------------打印日志、状态机、快照信息---------------------------");
            logModule.printAll();
            stateMachine.printAll();
            printSnapshot();
            System.out.println("\n");
        }
    }

    private void printSnapshot() {
        SnapshotMetadata metadata = snapshot.getMetadata();
        if (metadata != null) {
            System.out.println("快照: " + metadata.toString());
        }
    }

}
