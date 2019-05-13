package com.raft.util;

import com.raft.pojo.SnapshotMetadata;

import java.io.*;

/**
 * created by Ethan-Walker on 2019/5/11
 */
public class FileUtils {

    public static void storeSnapshotMetadata(File file, SnapshotMetadata data) {
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file))) {
            outputStream.writeObject(data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static SnapshotMetadata readSnapshotMetadata(File file) {
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file))) {
            return (SnapshotMetadata) inputStream.readObject();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
