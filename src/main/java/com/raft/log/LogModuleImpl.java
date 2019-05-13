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
    private final static byte[] LAST_SNAPSHOT_INDEX = "LAST_SNAPSHOT_INDEX".getBytes();
    private final static byte[] LAST_SNAPSHOT_TERM = "LAST_SNAPSHOT_TERM".getBytes();

    private final static String dbDir;
    private final static String logsDir;
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

    //写入LogEntry
    public void write(LogEntry entry) {
        lock.lock();
//        System.out.println("-----------to write: " + entry + "------------");
        try {
            long lastIndex = getLastIndex();
//            System.out.println("lastIndex: " + lastIndex);
            entry.setIndex(lastIndex + 1);
            rocksDB.put(convertToBytes(lastIndex + 1), JSON.toJSONBytes(entry));
            updateLastIndex(lastIndex + 1);

//            System.out.println("update lastIndex: " + getLastIndex());
//            System.out.println("after append ,last entry= " + getLast());
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
//        System.out.println("--------write-over----------------");
    }

    //由索引值得到LogEntry
    public LogEntry read(long index) {
        if (index == getLastSnapshotIndex()) {
            LogEntry entry = new LogEntry();
            entry.setIndex(getLastSnapshotIndex());
            entry.setTerm(getLastSnapshotTerm());
            return entry;
        }
        try {
            byte[] res = rocksDB.get(convertToBytes(index));

            if (res != null) {
                return JSON.parseObject(res, LogEntry.class);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    //获取最新的LogEntry
    public LogEntry getLast() {
        long lastIndex = getLastIndex();
        try {
            if (lastIndex == -1) return null;
            byte[] result = rocksDB.get(convertToBytes(lastIndex));
            if (result != null) {
                LogEntry entry = JSON.parseObject(result, LogEntry.class);
                return entry;
            } else {
                // 该日志被生成快照了
//                System.out.println("该日志被生成快照了");
                LogEntry entry = new LogEntry();
                entry.setTerm(getLastSnapshotTerm());
                entry.setIndex(getLastSnapshotIndex());
                return entry;
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void removeRange(long start, long end) {
        lock.lock();
        long i = 0;
        try {
            for (i = start; i <= end; i++) {
                rocksDB.delete(String.valueOf(i).getBytes());
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void updateLastSnapshotIndex(long index) {
        try {
            rocksDB.put(LAST_SNAPSHOT_INDEX, convertToBytes(index));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getLastSnapshotIndex() {
        byte[] bys = "-1".getBytes();
        try {
            bys = rocksDB.get(LAST_SNAPSHOT_INDEX);
            if (bys == null) {
                bys = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.parseLong(new String(bys));
    }

    //删除从index之后所有的日志条目
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

    @Override
    public int getLastSnapshotTerm() {
        byte[] lastTerm = "-1".getBytes();
        try {
            lastTerm = rocksDB.get(LAST_SNAPSHOT_TERM);
            if (lastTerm == null) {
                lastTerm = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(new String(lastTerm));
    }

    //获取最新的index
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

    public byte[] convertToBytes(Integer a) {
        return a.toString().getBytes();
    }

    //更新最新日志条目的index
    public void updateLastIndex(long index) {
        try {
            rocksDB.put(LAST_INDEX_KEY, convertToBytes(index));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    //更新最新日志条目的term
    public void updateLastSnapshotTerm(int term) {
        try {
            rocksDB.put(LAST_SNAPSHOT_TERM, convertToBytes(term));
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
        System.out.println("---------日志:lastSnapshotIndex= " + getLastSnapshotIndex() + ",lastSnapshotTerm=" + getLastSnapshotTerm() + ", lastIndex = " + lastIndex + "--------");
        for (long i = getLastSnapshotIndex() + 1; i <= lastIndex; i++) {
            LogEntry entry = read(i);
            System.out.println(entry.toString());
        }
    }
}
