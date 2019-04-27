package com.raft;

import com.alibaba.fastjson.JSON;
import com.raft.pojo.Peer;
import com.raft.pojo.Response;
import com.raft.rpc.ManageRPCServer;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * created by Ethan-Walker on 2019/4/17
 */
public class ManageServer {

    private static RocksDB db;

    private final String dbDir = "./cluster";


    private final byte[] PEER_SET = "PEER_INDEX_SET".getBytes();

    private ManageRPCServer rpcServer;

    private int selfPort;

    private ScheduledExecutorService threadPool;

    private Map<Peer, Long> peerStampMap;

    private long peerTimeout = 10 * 1000; // 超过 10s 会认为当前节点宕机，从集群中移出

    public ManageServer(int port) {
        this.selfPort = port;
    }

    public void init() {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            db = RocksDB.open(options, dbDir);
            List<Peer> indexes = new ArrayList<>();
            db.put(PEER_SET, JSON.toJSONBytes(indexes));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        rpcServer = new ManageRPCServer(selfPort, this);
        rpcServer.start();
        peerStampMap = new HashMap<>();
        threadPool = Executors.newScheduledThreadPool(2);
        threadPool.scheduleAtFixedRate(new RegisterAddrCheckTask(), 0, 2000, TimeUnit.MILLISECONDS);
        threadPool.scheduleAtFixedRate(new PrintTask(), 0, 5000, TimeUnit.MILLISECONDS);

    }

    /**
     * 检查各个节点的时间戳是否过期
     */
    class RegisterAddrCheckTask implements Runnable {
        @Override
        public void run() {
            HashSet<Peer> peers = getPeers();
            if (peers != null) {
                long current = System.currentTimeMillis();
                Iterator<Peer> iterator = peers.iterator();
                boolean hasChange = false;
                while (iterator.hasNext()) {
                    Peer p = iterator.next();
                    if (current - peerStampMap.get(p) > peerTimeout) {
                        iterator.remove();
                        System.out.println(p + " 签到超时被移除");
                        hasChange = true;
                    }
                }
                if (hasChange) {
                    updatePeers(peers);
                }
            }
        }
    }

    /**
     * 定时签名心跳
     *
     * @param p
     * @return
     */
    public Response sign(Peer p) {
        Response response = new Response();
        if (peerStampMap.get(p) == null) {
            response.setObj(false);
        } else {
            peerStampMap.put(p, System.currentTimeMillis());
            response.setObj(true);
        }
        return response;
    }

    private void updatePeers(HashSet<Peer> peers) {
        try {
            db.put(PEER_SET, JSON.toJSONBytes(peers));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public Response getAllPeers() {
        HashSet<Peer> peers = getPeers();
        Response response = new Response();
        response.setObj(new ArrayList<>(peers));
        return response;
    }

    public Response getRandomPeer() {
        HashSet<Peer> allPeers = getPeers();
        int random = new Random().nextInt(allPeers.size());
        Response response = new Response();
        response.setObj(new ArrayList<>(allPeers).get(random));
        return response;
    }

    public synchronized Response registerPeer(Peer p) {
        HashSet<Peer> peers = getPeers();
        Response response = new Response();
        response.setObj(true);
        if (!peers.contains(p)) {
            peers.add(p);
            try {
                db.put(PEER_SET, JSON.toJSONBytes(peers));
                System.out.println("节点 " + p.getAddr() + "来注册");
                peerStampMap.put(p, System.currentTimeMillis());
            } catch (RocksDBException e) {
                e.printStackTrace();
                response.setObj(false);
            }
        } else {
            peerStampMap.put(p, System.currentTimeMillis());
        }
        return response;
    }

    private HashSet<Peer> getPeers() {
        List<Peer> peerList = new ArrayList<>();
        try {
            peerList = JSON.parseArray(new String(db.get(PEER_SET)), Peer.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return new HashSet<>(peerList);

    }


    class PrintTask implements Runnable {
        @Override
        public void run() {
            HashSet<Peer> peers = getPeers();
            System.out.println("PeerList : " + peers);
        }
    }
}
