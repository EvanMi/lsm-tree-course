package com.yumi.lsm.filter;

import com.yumi.lsm.filter.bloom.BloomFilter;

public class LsmBloomFilter implements Filter{
    private final BloomFilter bloomFilter = BloomFilter.createByFn(20, 400);
    private BitsArray bitsArray = BitsArray.create(bloomFilter.getM());

    private Integer keyCnt = 0;

    @Override
    public void add(byte[] key) {
        this.keyCnt++;
        this.bloomFilter.hashTo(this.bloomFilter.calcBitPositions(key), this.bitsArray);
    }

    @Override
    public BitsArray hash() {
        return this.bitsArray.clone();
    }

    @Override
    public boolean exist(byte[] key, BitsArray bitsArray) {
        return this.bloomFilter.isHit(this.bloomFilter.calcBitPositions(key), bitsArray);
    }

    @Override
    public void reset() {
        this.bitsArray = BitsArray.create(this.bloomFilter.getM());
        this.keyCnt = 0;
    }

    @Override
    public int keyLen() {
        return this.keyCnt;
    }
}
