package com.raft.log;

import com.alibaba.fastjson.JSON;
import com.raft.pojo.LogEntry;
import com.sun.org.apache.xerces.internal.impl.xs.opti.DefaultNode;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class LogModuleImpl implements LogModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    private final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();
    private static String dbDir;
    private static String logsDir;
    private static RocksDB rocksDB;

    private ReentrantLock lock;

    static {
        dbDir = "./db/" + System.getProperty("serverPort");
        logsDir = dbDir + "/logs";
        RocksDB.loadLibrary(); // 初始化
    }

    public LogModuleImpl() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        File file = new File(logsDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            rocksDB = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        lock = new ReentrantLock();
    }

    public void write(LogEntry entry) {
        lock.lock();
        System.out.println("-----------to write: " + entry + "------------");
        try {
            long lastIndex = getLastIndex();
            System.out.println("lastIndex: " + lastIndex);
            entry.setIndex(lastIndex + 1);
            rocksDB.put(convertToBytes(lastIndex + 1), JSON.toJSONBytes(entry));
            updateLastIndex(lastIndex + 1);

            System.out.println("update lastIndex: " + getLastIndex());
            System.out.println("after append ,last entry= " + getLast());
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        System.out.println("--------write-over----------------");
    }

    public LogEntry read(long index) {
        try {
            byte[] res = rocksDB.get(convertToBytes(index));
            if (res != null)
                return (LogEntry) JSON.parseObject(res, LogEntry.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public LogEntry getLast() {
        long lastIndex = getLastIndex();
        try {
            if (lastIndex == -1) return null;
            byte[] result = rocksDB.get(convertToBytes(lastIndex));
            LogEntry entry = JSON.parseObject(result, LogEntry.class);
            return entry;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void removeFromIndex(long index) {
        lock.lock();
        long lastIndex = getLastIndex();
        boolean success = false;// 是否删除成功
        try {
            for (long i = index; i <= lastIndex; i++) {
                rocksDB.delete(String.valueOf(i).getBytes());
            }
            success = true;
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            if (success) {
                updateLastIndex(index - 1);
            }
            lock.unlock();
        }
    }

    public Long getLastIndex() {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = rocksDB.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.parseLong(new String(lastIndex));
    }

    public byte[] convertToBytes(Long a) {
        return a.toString().getBytes();
    }

    public void updateLastIndex(long index) {
        try {
            rocksDB.put(LAST_INDEX_KEY, convertToBytes(index));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void printAll() {
        long lastIndex = getLastIndex();
        if (lastIndex <= -1)
            return;
        // 没有就不输出
        System.out.println("-----" + logsDir + "--------------");
        System.out.println("lastIndex: " + lastIndex);
        for (long i = 0; i <= lastIndex; i++) {
            LogEntry entry = read(i);
            System.out.println(entry.toString());
        }
    }
}
