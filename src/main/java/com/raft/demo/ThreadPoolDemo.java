package com.raft.demo;

import com.alibaba.fastjson.JSON;
import com.raft.pojo.LogEntry;
import com.sun.org.apache.xerces.internal.impl.xs.opti.DefaultNode;
import org.omg.PortableInterceptor.LOCATION_FORWARD;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class ThreadPoolDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    private final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();

    public static void main(String[] args) {
        // 周期性读取各节点的 日志信息，打印

        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);

        List<String> logsDirs = new ArrayList<>();
        List<Integer> ports = Arrays.asList(8001, 8002, 8003, 8004, 8005);

        logsDirs.add("./db/" + ports.get(0) + "/logs");
        logsDirs.add("./db/" + ports.get(1) + "/logs");
        logsDirs.add("./db/" + ports.get(2) + "/logs");
        logsDirs.add("./db/" + ports.get(3) + "/logs");
        logsDirs.add("./db/" + ports.get(4) + "/logs");

        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB.loadLibrary();
        try {
            final RocksDB rocksDB1 = RocksDB.open(options, logsDirs.get(0));
            final RocksDB rocksDB2 = RocksDB.open(options, logsDirs.get(1));
            final RocksDB rocksDB3 = RocksDB.open(options, logsDirs.get(2));
            final RocksDB rocksDB4 = RocksDB.open(options, logsDirs.get(3));
            final RocksDB rocksDB5 = RocksDB.open(options, logsDirs.get(4));
            threadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    access(rocksDB1, ports.get(0));
                    access(rocksDB2, ports.get(1));
                    access(rocksDB3, ports.get(2));
                    access(rocksDB4, ports.get(3));
                    access(rocksDB5, ports.get(4));
                    System.out.println();
                }
            }, 3000, 3000, TimeUnit.MILLISECONDS);

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void access(RocksDB db, int port) {
        long lastIndex = getLastIndex(db);
        LOGGER.info("----------start------" + port + "-----------");
        for (long i = 0; i <= lastIndex; i++) {
            LogEntry entry = read(db, i);
            LOGGER.info(entry.toString());
        }
    }

    public static Long getLastIndex(RocksDB db) {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = db.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.parseLong(new String(lastIndex));
    }

    public static LogEntry read(RocksDB db, long index) {
        try {
            byte[] res = db.get(convertToBytes(index));
            if (res != null)
                return (LogEntry) JSON.parseObject(res, LogEntry.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] convertToBytes(Long a) {
        return a.toString().getBytes();
    }

}
