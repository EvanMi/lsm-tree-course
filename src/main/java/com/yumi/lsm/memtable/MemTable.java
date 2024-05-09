package com.yumi.lsm.memtable;

import com.yumi.lsm.util.Kv;

import java.util.List;
import java.util.Optional;

public interface MemTable {
    void put(byte[] key, byte[] value);
    Optional<byte[]> get(byte[] key);
    //table 中所有数据的大小 单位是byte
    int size();
    //key-value 数量
    int entriesCnt();
    List<Kv> all();
}
