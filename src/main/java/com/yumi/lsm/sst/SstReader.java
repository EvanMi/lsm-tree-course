package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.util.BufferCleanUtil;
import com.yumi.lsm.util.Kv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SstReader {
    private Config config;
    private RandomAccessFile src;
    private FileChannel channel;

    private int filterOffset;
    private int filterSize;
    private int indexOffset;
    private int indexSize;

    public SstReader(String file, Config config) {
        File jFile = new File(config.getDir() + File.separator + file);
        if (!jFile.exists()) {
            throw new IllegalStateException("文件不存在 " + file);
        }
        this.config = config;
        try {
            this.src = new RandomAccessFile(jFile, "r");
            this.channel = this.src.getChannel();
        } catch (FileNotFoundException e) {
            IllegalStateException illegalStateException = new IllegalStateException("文件不存在 " + file);
            illegalStateException.addSuppressed(e);
            throw illegalStateException;
        }
    }

    public void readFooter() {
        try {
            long length = this.src.length();
            int bufferSize = this.config.getSstFooterSize();
            MappedByteBuffer slice = this.channel.map(FileChannel.MapMode.READ_ONLY,
                    (int) length - bufferSize, bufferSize);
            this.filterOffset = slice.getInt();
            this.filterSize = slice.getInt();
            this.indexOffset = slice.getInt();
            this.indexSize = slice.getInt();
            BufferCleanUtil.clean(slice);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public Map<Integer, BitsArray> readFilter() {
        if (filterOffset == 0 || filterSize == 0) {
            readFooter();
        }
        ByteBuffer byteBuffer = this.readBlock(this.filterOffset, this.filterSize);
        Map<Integer, BitsArray> ans = bufferToFilter(byteBuffer);
        BufferCleanUtil.clean(byteBuffer);
        return ans;
    }

    public Index[] readIndex() {
        if (indexOffset == 0 || indexSize == 0) {
            readFooter();
        }
        ByteBuffer byteBuffer = this.readBlock(this.indexOffset, this.indexSize);
        Index[] indices = bufferToIndex(byteBuffer);
        BufferCleanUtil.clean(byteBuffer);
        return indices;
    }

    public Kv[] readData() {
        if (indexOffset == 0 || indexSize == 0 || filterOffset == 0 || filterSize == 0) {
            readFooter();
        }
        return readData(0, filterOffset);
    }

    public Kv[] readData(int offset, int size) {
        ByteBuffer byteBuffer = this.readBlock(offset, size);
        Kv[] ans = readBlockData(byteBuffer);
        BufferCleanUtil.clean(byteBuffer);
        return ans;
    }

    public Kv[] readBlockData(ByteBuffer byteBuffer) {
        byte[] preKey = new byte[0];
        List<Kv> dataList = new ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            Kv kv = this.readRecord(preKey, byteBuffer);
            dataList.add(kv);
            preKey = kv.getKey();
        }
        return dataList.toArray(new Kv[0]);
    }

    public int size() {
        if (this.indexOffset == 0) {
            this.readFooter();
        }
        return this.indexOffset + this.indexSize + config.getSstFooterSize();
    }

    public void close() {
        try {
            this.channel.close();
            this.src.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Index[] bufferToIndex(ByteBuffer byteBuffer) {
        List<Index> indexList = new ArrayList<>();
        byte[] preKey = new byte[0];
        while(byteBuffer.hasRemaining()) {
            Kv kv = this.readRecord(preKey, byteBuffer);
            ByteBuffer wrap = ByteBuffer.wrap(kv.getValue());
            int offset = wrap.getInt();
            int size = wrap.getInt();
            Index index = new Index();
            index.setLastKey(kv.getKey());
            index.setBlockOffset(offset);
            index.setBlockSize(size);
            indexList.add(index);
            preKey = kv.getKey();
        }
        return indexList.toArray(new Index[0]);
    }


    private Map<Integer, BitsArray> bufferToFilter(ByteBuffer byteBuffer) {
        Map<Integer, BitsArray> bufferToFilter = new HashMap<>();
        byte[] preKey = new byte[0];
        while (byteBuffer.hasRemaining()) {
            Kv kv = this.readRecord(preKey, byteBuffer);
            int offset = ByteBuffer.wrap(kv.getKey()).getInt();
            bufferToFilter.put(offset, BitsArray.create(kv.getValue()));
            preKey = kv.getKey();
        }
        return bufferToFilter;
    }

    private Kv readRecord(byte[] preKey, ByteBuffer byteBuffer) {
        int sharedKeyLen = byteBuffer.getInt();
        int suffixKeyLen = byteBuffer.getInt();
        int valueLen = byteBuffer.getInt();

        byte[] suffixKey = new byte[suffixKeyLen];
        byteBuffer.get(suffixKey);

        byte[] value = new byte[valueLen];
        byteBuffer.get(value);

        byte[] key = new byte[sharedKeyLen + suffixKeyLen];
        System.arraycopy(preKey, 0, key, 0, sharedKeyLen);
        System.arraycopy(suffixKey, 0, key, sharedKeyLen, suffixKeyLen);
        return new Kv(key, value);
    }


    private ByteBuffer readBlock(int offset, int size) {
        try {
            return this.channel.map(FileChannel.MapMode.READ_ONLY, offset, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getFilterOffset() {
        return filterOffset;
    }

    public int getFilterSize() {
        return filterSize;
    }

    public int getIndexOffset() {
        return indexOffset;
    }

    public int getIndexSize() {
        return indexSize;
    }
}
