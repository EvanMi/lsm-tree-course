package com.yumi.lsm;

import com.yumi.lsm.memtable.MemTable;
import com.yumi.lsm.sst.Node;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.wal.WalWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree {
    private Config config;
    private final ReadWriteLock dataLock = new ReentrantReadWriteLock();
    private int memTableIndex = 0;
    private WalWriter walWriter;
    private MemTable memtable;
    private List<MemTableCompactItem> readOnlyMemTableList = new ArrayList<>();
    private List<List<Node>> nodes;
    private ReentrantReadWriteLock[] levelLocks;

    public Tree(Config config) {
        this.config = config;
        this.nodes = new ArrayList<>();
        for (int i = 0; i < config.getMaxLevel(); i++) {
            nodes.add(new ArrayList<>());
        }
        this.levelLocks = new ReentrantReadWriteLock[config.getMaxLevel()];
        for (int i = 0; i < levelLocks.length; i++) {
            levelLocks[i] = new ReentrantReadWriteLock();
        }
        newMemTable(config.getWalFileSize());
    }

    public void put(byte[] key, byte[] value) {
        Lock lock = dataLock.writeLock();
        lock.lock();;
        try {
            //1.写入wal file
            boolean notFull = this.walWriter.write(key, value);
            // --写满了要重建
            if (!notFull) {
                refreshMemTable();
                notFull = this.walWriter.write(key, value);
                assert notFull;
            }
            //2.写入mem table中
            this.memtable.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void refreshMemTable() {
        //热表转冷表
        MemTableCompactItem oldItem = new MemTableCompactItem(this.walFile(), this.memtable);
        //放到冷表队列中
        readOnlyMemTableList.add(oldItem);
        this.walWriter.close();
        // TODO: 异步方式把冷表持久化为0层的sst文件
        this.memTableIndex++;
        this.newMemTable(this.config.getWalFileSize());
    }

    private void newMemTable(int size) {
        this.walWriter = new WalWriter(walFile(), size);
        this.memtable = this.config.getMemTableConstructor().create();
    }

    private String walFile() {
        return config.getDir() + File.separator + "wal" + File.separator + this.memTableIndex + ".wal";
    }


    public byte[] get(byte[] key) {
        Lock lock = this.dataLock.readLock();
        lock.lock();
        try {
            //mem table找
            Optional<byte[]> valOpt = this.memtable.get(key);
            if (valOpt.isPresent()) {
                return valOpt.get();
            }
            //从新到旧冷表找
            int oldLen = this.readOnlyMemTableList.size();
            for (int i = oldLen - 1; i >= 0; i--) {
                MemTableCompactItem memTableCompactItem = this.readOnlyMemTableList.get(i);
                Optional<byte[]> oldOpt = memTableCompactItem.getMemTable().get(key);
                if (oldOpt.isPresent()) {
                    return oldOpt.get();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        //从新到旧0层的sst文件找
        ReentrantReadWriteLock.ReadLock level0Lock = this.levelLocks[0].readLock();
        try {
            List<Node> level0Nodes = this.nodes.get(0);
            int level0Len = level0Nodes.size();
            for (int i = level0Len - 1; i >= 0; i--) {
                Node node = level0Nodes.get(i);
                Optional<byte[]> level0Opt = node.get(key);
                if (level0Opt.isPresent()) {
                    return level0Opt.get();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            level0Lock.unlock();
        }
        //从1层开始，逐层寻找，可以二分查找
        for (int level = 1; level < this.nodes.size(); level++) {
            ReentrantReadWriteLock.ReadLock levelLock = this.levelLocks[level].readLock();
            levelLock.lock();
            try {
                Optional<Node> nodeOpt = this.levelBinarySearch(level, key, 0, this.nodes.get(level).size() - 1);
                if (!nodeOpt.isPresent()) {
                    continue;
                }
                Optional<byte[]> valOpt = nodeOpt.get().get(key);
                if (valOpt.isPresent()) {
                    return valOpt.get();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                levelLock.unlock();
            }
        }
        return null;
    }

    private Optional<Node> levelBinarySearch(int level, byte[] key, int start, int end) {
        if (end < start) {
            return Optional.empty();
        }

        int mid = start + ((end - start) >> 1);
        List<Node> levelNodes = this.nodes.get(level);
        Node midNode = levelNodes.get(mid);
        if (AllUtils.compare(midNode.end(), key) >= 0 && AllUtils.compare(midNode.start(), key) <= 0) {
            return Optional.of(midNode);
        }
        if (AllUtils.compare(midNode.start(), key) > 0) {
            if (mid > 0 && AllUtils.compare(key, levelNodes.get(mid - 1).end()) > 0 || mid == 0) {
                return Optional.of(midNode);
            } else {
                return this.levelBinarySearch(level, key, start, mid - 1);
            }
        }
        if (AllUtils.compare(midNode.end(), key) < 0) {
            if (mid < end - 1 && AllUtils.compare(key, levelNodes.get(mid + 1).start()) < 0 || mid == end - 1) {
                return Optional.of(midNode);
            } else {
                return this.levelBinarySearch(level, key, mid + 1, end);
            }
        }
        return Optional.empty();
    }


    public static class MemTableCompactItem {
        private final String walFile;
        private final MemTable memTable;

        public MemTableCompactItem(String walFile, MemTable memTable) {
            this.walFile = walFile;
            this.memTable = memTable;
        }

        public String getWalFile() {
            return walFile;
        }

        public MemTable getMemTable() {
            return memTable;
        }
    }
}
