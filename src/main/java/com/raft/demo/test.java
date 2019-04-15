package com.raft.demo;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * created by Ethan-Walker on 2019/4/15
 */
public class test {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);
        String logsDir = "./db/8001/logs";
        final byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();
        try {
            RocksDB db = RocksDB.open(options, logsDir);

            byte[] lastIndexBys = db.get(LAST_INDEX_KEY);
            System.out.println(Long.parseLong(new String(lastIndexBys)));

            db.put(LAST_INDEX_KEY, "0".getBytes());

            byte[] lastIndexBys2 = db.get(LAST_INDEX_KEY);
            System.out.println(Long.parseLong(new String(lastIndexBys2)));


        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }
}
