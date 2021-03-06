package com.raft.snapshot;

import com.raft.pojo.SnapshotMetadata;
import com.raft.util.MFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;

/**
 * created by Ethan-Walker on 2019/5/10
 */
public class SnapshotImpl implements Snapshot {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotImpl.class);
    private static final String dbDir;
    private static final String snapshotDir;
    private static final String metadataPath;

    static {
        dbDir = "./db/" + System.getProperty("serverPort");
        snapshotDir = dbDir + File.separator + "snapshot";
        metadataPath = snapshotDir + File.separator + "metadata";
    }

    public SnapshotImpl() {
        File file = new File(snapshotDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * 更新快照
     */
    @Override
    public void updateMetadata(long lastIncludedIndex, int lastIncludedTerm, LinkedHashMap<String, String> data) {
        File file = new File(metadataPath);
        // 构造快照文件数据
        SnapshotMetadata metadata = new SnapshotMetadata(lastIncludedIndex, lastIncludedTerm, data);
        MFileUtils.storeSnapshotMetadata(file, metadata);
    }

    @Override
    public SnapshotMetadata getMetadata() {
        File file = new File(metadataPath);
        if (!file.exists()) {
            return null;
        }
        SnapshotMetadata metadata = MFileUtils.readSnapshotMetadata(file);
        return metadata;
    }
}
