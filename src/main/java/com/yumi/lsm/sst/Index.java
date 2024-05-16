package com.yumi.lsm.sst;

import java.util.Arrays;
import java.util.Objects;

public class Index {
    private byte[] lastKey;
    private int blockOffset;
    private int blockSize;

    public byte[] getLastKey() {
        return lastKey;
    }

    public void setLastKey(byte[] lastKey) {
        this.lastKey = lastKey;
    }

    public int getBlockOffset() {
        return blockOffset;
    }

    public void setBlockOffset(int blockOffset) {
        this.blockOffset = blockOffset;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return blockOffset == index.blockOffset && blockSize == index.blockSize && Arrays.equals(lastKey, index.lastKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(blockOffset, blockSize);
        result = 31 * result + Arrays.hashCode(lastKey);
        return result;
    }
}
