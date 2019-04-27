
package com.raft.statemachine;

import com.raft.pojo.Command;
import com.raft.pojo.LogEntry;
import com.sun.org.apache.xerces.internal.impl.xs.opti.DefaultNode;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

public class StateMachineImpl implements StateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);
    private static final byte[] LAST_APPLIED = "LAST_APPLIED".getBytes();

    private static String dbDir;
    private static String smDir;
    private static RocksDB rocksDB;

    private ReentrantLock lock;

    static {
        dbDir = "./db/" + System.getProperty("serverPort");
        smDir = dbDir + "/stateMachine";
        RocksDB.loadLibrary();
    }

    public StateMachineImpl() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        File file = new File(smDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            rocksDB = RocksDB.open(options, smDir);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        lock = new ReentrantLock();
    }

    //由key获取value

    public String getValue(String key) {
        String s = null;
        try {
            byte[] value = rocksDB.get(key.getBytes());
            s = new String(value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return s;
    }

    ;

    //设置指定key的value
    @Override
    public void setValue(String key, String value) {
        try {
            rocksDB.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    ;

    @Override
    //删除多个key对应的value
    public void delValue(String... key) {
        try {
            for (String s : key) {
                rocksDB.delete(s.getBytes());
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    //应用到状态机
    @Override
    public void apply(LogEntry logEntry) {
        lock.lock();
//        System.out.println("-----------to apply: " + logEntry.getCommand().toString());
        try {
            Command command = logEntry.getCommand();
            if (command == null) {
                throw new IllegalArgumentException("command can't be null, logentry:" + logEntry.toString());
            }
            String key = logEntry.getCommand().getKey();
            String value = logEntry.getCommand().getValue();
            //存入rocksdb
            rocksDB.put(key.getBytes(), value.getBytes());
            rocksDB.put(LAST_APPLIED, convertToBytes(logEntry.getIndex()));

        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
//        System.out.println("--------apply-over----------------");
    }

    @Override
    public void print() {
        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seekToFirst();
        while (rocksIterator.isValid()) {
            byte[] key = rocksIterator.key();
            byte[] value = rocksIterator.value();
            System.out.println(new String(key) + "=" + new String(value));
            rocksIterator.next();
        }
    }

    public long getLastApplied() {
        byte[] bys = "-1".getBytes();
        try {
            bys = rocksDB.get(LAST_APPLIED);
            if (bys == null) {
                bys = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.valueOf(new String(bys));
    }

    public byte[] convertToBytes(long index) {
        return String.valueOf(index).getBytes();
    }
}