package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.util.Kv;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class Node {
    private Config config;
    private String file;
    //大小，单位byte
    private int size;
    private Map<Integer, BitsArray> blockToFilter;
    private Index[] indices;
    private byte[] startKey;
    private byte[] endKey;

    private SstReader sstReader;

    public Node(Config config, String file, SstReader sstReader, int size,
                Map<Integer, BitsArray> blockToFilter, Index[] indices) {
        this.config = config;
        this.file = file;
        this.sstReader = sstReader;
        this.blockToFilter = blockToFilter;
        this.indices = indices;
        this.size = size;

        this.startKey = this.sstReader.readData(indices[0].getBlockOffset(), indices()[0].getBlockSize())[0]
                .getKey();
        this.endKey = this.indices[this.indices.length - 1].getLastKey();
    }



    public Optional<byte[]> get(byte[] key) {
        Optional<Index> indexOptional = binarySearchIndex(key, 0, this.indices.length - 1);
        if (!indexOptional.isPresent()) {
            return Optional.empty();
        }
        Index index = indexOptional.get();
        BitsArray bitsArray = this.blockToFilter.get(index.getBlockOffset());
        assert bitsArray != null;
        if (!this.config.getFilter().exist(key, bitsArray)) {
            return Optional.empty();
        }
        Kv[] kvs = this.sstReader.readData(index.getBlockOffset(), index.getBlockSize());
        byte[] value = binarySearchKv(kvs, 0, kvs.length - 1, key);
        return Optional.of(value);
    }

    private byte[] binarySearchKv(Kv[] kvs, int l, int h, byte[] targetKey) {
        if (l == h) {
            assert AllUtils.compare(kvs[l].getKey(), targetKey) == 0;
            return kvs[l].getValue();
        }
        int mid = l + ((h - l) >> 1);
        byte[] midKey = kvs[mid].getKey();
        if (AllUtils.compare(midKey, targetKey) == 0) {
            return kvs[mid].getValue();
        } else if (AllUtils.compare(midKey, targetKey) < 0) {
            return binarySearchKv(kvs, mid + 1, l, targetKey);
        } else {
            return binarySearchKv(kvs, l, mid -1 , targetKey);
        }
    }

    private Optional<Index> binarySearchIndex(byte[] key, int l, int h) {
        if (l == h) {
            if (AllUtils.compare(indices[l].getLastKey(), key) >= 0) {
                return Optional.of(indices[l]);
            }
            return Optional.empty();
        }

        int mid = l + ((h - l) >> 2);
        if (AllUtils.compare(indices[mid].getLastKey(), key) < 0) {
            return binarySearchIndex(key, mid + 1, h);
        } else {
            return binarySearchIndex(key, l, mid);
        }
    }


    public Kv[] getRange(int offset, int size) {
        return this.sstReader.readData(offset, size);
    }

    public int size() {
        return size;
    }

    public byte[] start() {
        return startKey;
    }

    public byte[] end() {
        return endKey;
    }

    public Index[] indices() {
        return indices;
    }

    //销毁，要删除对应的文件
    public void destroy() {
        this.close();
        new File(this.config.getDir() + File.separator + this.file).delete();
    }

    //关闭读流
    public void close() {
        this.sstReader.close();
    }


}
