package com.yumi.lsm;

import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.memtable.MemTable;
import com.yumi.lsm.sst.Index;
import com.yumi.lsm.sst.Node;
import com.yumi.lsm.sst.SstReader;
import com.yumi.lsm.sst.SstWriter;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.util.Kv;
import com.yumi.lsm.wal.WalWriter;
import org.jctools.queues.SpscArrayQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree {
    private Config config;
    private final ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();
    private int memTableIndex = 0;
    private WalWriter walWriter;
    private MemTable memtable;
    private List<MemTableCompactItem> readOnlyMemTableList = new ArrayList<>();
    private List<List<Node>> nodes;
    private ReentrantReadWriteLock[] levelLocks;

    private SpscArrayQueue<MemTableCompactItem> memCompactQueue = new SpscArrayQueue<>(500);
    private SpscArrayQueue<Integer> levelCompactQueue = new SpscArrayQueue<>(200);
    private AtomicBoolean stop = new AtomicBoolean(false);
    private AtomicInteger[] levelToSeq;

    public Tree(Config config) {
        this.config = config;
        this.nodes = new ArrayList<>();
        this.levelToSeq = new AtomicInteger[config.getMaxLevel()];
        for (int i = 0; i < levelToSeq.length; i++) {
            levelToSeq[i] = new AtomicInteger(0);
        }
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
        ReentrantReadWriteLock.WriteLock lock = dataLock.writeLock();
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
        while (true) {
            boolean offer = this.memCompactQueue.offer(oldItem);
            if (offer) {
                break;
            }
            Thread.yield();
        }
        this.memTableIndex++;
        this.newMemTable(this.config.getWalFileSize());
    }

    private void doBackendTask() {
        while (!stop.get()) {
            MemTableCompactItem item = memCompactQueue.poll();
            if (null != item) {
                this.compactMemTable(item);
            }
        }
    }

    private void compactMemTable(MemTableCompactItem item) {
        flushMemTable(item.memTable);
        ReentrantReadWriteLock.WriteLock writeLock = this.dataLock.writeLock();
        writeLock.lock();
        try {
            List<MemTableCompactItem> rms = this.readOnlyMemTableList;
            int rmsSize = rms.size();
            for (int i = 0; i < rmsSize; i++) {
                if (rms.get(i).getMemTable() != item.getMemTable()) {
                    new File(rms.get(i).getWalFile()).delete();
                    continue;
                }
                this.readOnlyMemTableList = new ArrayList<>(rms.subList(i + 1, rmsSize));
                new File(rms.get(i).getWalFile()).delete();
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
    }

    private void flushMemTable(MemTable memTable) {
        int seq = levelToSeq[0].get() + 1;
        SstWriter sstWriter = new SstWriter(sstFile(0, seq), config);
        try {
            for (Kv kv : memTable.all()) {
                sstWriter.append(kv.getKey(), kv.getValue());
            }
            SstWriter.FinishRes finish = sstWriter.finish();
            //插入内存表示node
            insertNode(0, seq, finish.getSize(), finish.getBlockToFilter(), finish.getIndices());
            //尝试发起下一层的压缩
            tryCompactSst(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sstWriter.close();
        }
    }

    private void tryCompactSst(int level) {
        if (level == this.nodes.size() - 1) {
            //最后一层不压缩
            return;
        }
        int size = 0;
        for (Node node : this.nodes.get(level)) {
            size += node.size();
        }
        if (size <= this.config.getLevelSstSize(level)) {
            return;
        }
        boolean offer = this.levelCompactQueue.offer(level);
        if (!offer) {
            System.out.println("压缩文件正忙");
        }
    }

    private void insertNode(int level, int seq, int size, Map<Integer, BitsArray> blockToFilter, Index[] indices) {
        String file = sstFile(level, seq);
        SstReader sstReader = new SstReader(file, config);
        this.insertNodeWithReader(level, seq, size, blockToFilter, indices, sstReader);
    }

    private void insertNodeWithReader(int level, int seq, int size, Map<Integer, BitsArray> blockToFilter, Index[] indices, SstReader sstReader) {
        String file = sstFile(level, seq);
        this.levelToSeq[level].set(seq);
        Node newNode = new Node(config, file, sstReader, size, blockToFilter, indices);
        ReentrantReadWriteLock.WriteLock writeLock = this.levelLocks[level].writeLock();
        writeLock.lock();
        try {
            if (level == 0) {
                this.nodes.get(level).add(newNode);
            } else {
                List<Node> levelNodes = nodes.get(level);
                if (levelNodes.isEmpty()) {
                    levelNodes.add(newNode);
                } else {
                    int i = 0;
                    for (; i < levelNodes.size(); i++) {
                        if (AllUtils.compare(newNode.end(), levelNodes.get(i).start()) < 0) {
                            levelNodes.add(i, newNode);
                        }
                    }
                    if (i == levelNodes.size()) {
                        levelNodes.add(newNode);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }

    }

    private String sstFile(int level, int seq) {
        return level + "_" + seq + ".sst";
    }

    private void newMemTable(int size) {
        this.walWriter = new WalWriter(walFile(), size);
        this.memtable = this.config.getMemTableConstructor().create();
    }

    private String walFile() {
        return config.getDir() + File.separator + "wal" + File.separator + this.memTableIndex + ".wal";
    }


    public byte[] get(byte[] key) {
        ReentrantReadWriteLock.ReadLock lock = this.dataLock.readLock();
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
