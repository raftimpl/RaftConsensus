package com.raft.demo;

import com.raft.pojo.SnapshotMetadata;
import com.raft.util.MFileUtils;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * created by Ethan-Walker on 2019/4/15
 */
public class test {
    public static void main(String[] args) {

        String logsDir = "./db/8001/snapshot/metadata";

        SnapshotMetadata metadata = MFileUtils.readSnapshotMetadata(new File(logsDir));
        System.out.println(metadata);


        List list = new ArrayList();
        list.add(null);

    }


    @Test
    public void test() {
        Object a = null;
        String b = (String) a;
        System.out.println(a);
    }
}
