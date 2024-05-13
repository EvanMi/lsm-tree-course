package com.yumi.lsm.memtable;

import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.util.Kv;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 并发情况下保证最终一致性 ->put 中的k-v放置 和 size的累加是两个分开的操作，所以
 * 获取到的size可能是真正的size，但最终会一致
 */
public class SkipListMemTable implements MemTable{
    private ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(
            (b1, b2) -> AllUtils.compare(b1,b2)
    );
    private AtomicInteger bytes = new AtomicInteger(0);

    @Override
    public void put(byte[] key, byte[] value) {
        byte[] old = this.map.put(key, value);
        int oldSize = null == old ? 0 : old.length;
        int keySize = null == old ? key.length : 0;
        this.bytes.addAndGet(keySize + value.length - oldSize);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.ofNullable(this.map.get(key));
    }

    @Override
    public int size() {
        return this.bytes.get();
    }

    @Override
    public int entriesCnt() {
        return this.map.size();
    }

    @Override
    public List<Kv> all() {
        return map.entrySet()
                .stream()
                .map((entry) -> new Kv(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
}
